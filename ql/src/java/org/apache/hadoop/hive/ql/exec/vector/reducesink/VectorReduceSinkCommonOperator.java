/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.TopNHash;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.ReduceRecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.Murmur3;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class is common operator class for native vectorized reduce sink.
 */
public abstract class VectorReduceSinkCommonOperator extends TerminalOperator<ReduceSinkDesc>
    implements Serializable, TopNHash.BinaryCollector,
    VectorizationOperator, VectorizationContextRegion {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkCommonOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private static final int NUM_ROWS_THRESHOLD_FOR_BATCH = 1;
  private static final int VECTOR_SHUFFLE_ACCUMULATE_ROW_THRESHOLD = 8;
  private static final int SERIALIZE_BUFFER_SIZE = 100 * 1024;

  /**
   * Information about our native vectorized reduce sink created by the Vectorizer class during
   * it decision process and useful for execution.
   */
  protected VectorReduceSinkInfo vectorReduceSinkInfo;

  protected VectorizationContext vContext;
  protected VectorReduceSinkDesc vectorDesc;

  /**
   * Reduce sink key vector expressions.
   */

  // This is map of which vectorized row batch columns are the key columns.
  // And, their types.
  protected boolean isEmptyKey;
  protected int[] reduceSinkKeyColumnMap;
  protected TypeInfo[] reduceSinkKeyTypeInfos;

  // Optional vectorized key expressions that need to be run on each batch.
  protected VectorExpression[] reduceSinkKeyExpressions;

  // This is map of which vectorized row batch columns are the value columns.
  // And, their types.
  protected boolean isEmptyValue;
  protected int[] reduceSinkValueColumnMap;
  protected TypeInfo[] reduceSinkValueTypeInfos;

  // Optional vectorized value expressions that need to be run on each batch.
  protected VectorExpression[] reduceSinkValueExpressions;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // Whether there is to be a tag added to the end of each key and the tag value.
  protected transient boolean reduceSkipTag;
  protected transient byte reduceTagByte;

  // Binary sortable key serializer.
  protected transient BinarySortableSerializeWrite keyBinarySortableSerializeWrite;

  // Lazy binary value serializer.
  protected transient LazyBinarySerializeWrite valueLazyBinarySerializeWrite;

  // This helper object serializes LazyBinary format reducer values from columns of a row
  // in a vectorized row batch.
  protected transient VectorSerializeRow<LazyBinarySerializeWrite> valueVectorSerializeRow;

  // The output buffer used to serialize a value into.
  protected transient Output valueOutput;

  // The hive key and bytes writable value needed to pass the key and value to the collector.
  protected transient HiveKey keyWritable;
  protected transient BytesWritable valueBytesWritable;

  // Picks topN K:V pairs from input.
  protected transient TopNHash reducerHash;

  // Where to write our key and value pairs.
  private transient TezProcessor.TezKVOutputCollector out;

  private transient long cntr = 1;

  // For debug tracing: the name of the map or reduce task.
  protected transient String taskName;

  // Debug display.
  protected transient long batchCounter;

  private transient boolean vectorShuffleBatchAllowed;
  private transient int vectorShuffleNumUnorderedPartitions;

  private transient VectorShuffleBatchSerializer vectorShuffleBatchSerializer;
  private transient byte[] vectorShuffleSerializeBuffer;
  private transient int[] vectorShuffleBatchColumnMap;
  private transient HiveKey vectorShuffleBatchKey;
  private transient BinarySortableSerializeWrite vectorShuffleRowKeySerializeWrite;
  private transient Output vectorShuffleRowKeyOutput;
  private transient VectorSerializeRow<BinarySortableSerializeWrite> vectorShuffleRowKeySerializeRow;

  // Scratch hash codes for active rows in the current batch, indexed by physical row.
  protected transient int[] batchKeyHashCodes;
  protected transient int[] batchValueHashCodes;
  private transient int[] vectorShufflePartitionCounts;
  private transient int[] vectorShufflePartitionOffsets;
  private transient int[] vectorShufflePartitionPositions;
  private transient int[] vectorShuffleRowPartitions;
  private transient int[] vectorShufflePartitionRowIndices;
  private transient byte[][] vectorShufflePendingBuffers;
  private transient int[] vectorShufflePendingBytes;
  private transient long[] vectorShufflePendingRows;
  private transient int vectorShufflePartitionerType;

  //---------------------------------------------------------------------------

  /** Kryo ctor. */
  protected VectorReduceSinkCommonOperator() {
    super();
  }

  public VectorReduceSinkCommonOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkCommonOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    this(ctx);

    ReduceSinkDesc desc = (ReduceSinkDesc) conf;
    this.conf = desc;
    this.vContext = vContext;
    this.vectorDesc = (VectorReduceSinkDesc) vectorDesc;
    vectorReduceSinkInfo = this.vectorDesc.getVectorReduceSinkInfo();

    isEmptyKey = this.vectorDesc.getIsEmptyKey();
    if (!isEmptyKey) {
      // Since a key expression can be a calculation and the key will go into a scratch column,
      // we need the mapping and type information.
      reduceSinkKeyColumnMap = vectorReduceSinkInfo.getReduceSinkKeyColumnMap();
      reduceSinkKeyTypeInfos = vectorReduceSinkInfo.getReduceSinkKeyTypeInfos();
      reduceSinkKeyExpressions = vectorReduceSinkInfo.getReduceSinkKeyExpressions();
    }

    isEmptyValue = this.vectorDesc.getIsEmptyValue();
    if (!isEmptyValue) {
      reduceSinkValueColumnMap = vectorReduceSinkInfo.getReduceSinkValueColumnMap();
      reduceSinkValueTypeInfos = vectorReduceSinkInfo.getReduceSinkValueTypeInfos();
      reduceSinkValueExpressions = vectorReduceSinkInfo.getReduceSinkValueExpressions();
    }
  }

  // Get the sort order
  private boolean[] getColumnSortOrder(Properties properties, int columnCount) {
    String columnSortOrder = properties.getProperty(serdeConstants.SERIALIZATION_SORT_ORDER);
    boolean[] columnSortOrderIsDesc = new boolean[columnCount];
    if (columnSortOrder == null) {
      Arrays.fill(columnSortOrderIsDesc, false);
    } else {
      for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
        columnSortOrderIsDesc[i] = (columnSortOrder.charAt(i) == '-');
      }
    }
    return columnSortOrderIsDesc;
  }

  private byte[] getColumnNullMarker(Properties properties, int columnCount, boolean[] columnSortOrder) {
    String columnNullOrder = properties.getProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER);
    byte[] columnNullMarker = new byte[columnCount];
      for (int i = 0; i < columnNullMarker.length; i++) {
        if (columnSortOrder[i]) {
          // Descending
          if (columnNullOrder != null && columnNullOrder.charAt(i) == 'a') {
            // Null first
            columnNullMarker[i] = BinarySortableSerDe.ONE;
          } else {
            // Null last (default for descending order)
            columnNullMarker[i] = BinarySortableSerDe.ZERO;
          }
        } else {
          // Ascending
          if (columnNullOrder != null && columnNullOrder.charAt(i) == 'z') {
            // Null last
            columnNullMarker[i] = BinarySortableSerDe.ONE;
          } else {
            // Null first (default for ascending order)
            columnNullMarker[i] = BinarySortableSerDe.ZERO;
          }
        }
    }
    return columnNullMarker;
  }

  private byte[] getColumnNotNullMarker(Properties properties, int columnCount, boolean[] columnSortOrder) {
    String columnNullOrder = properties.getProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER);
    byte[] columnNotNullMarker = new byte[columnCount];
      for (int i = 0; i < columnNotNullMarker.length; i++) {
        if (columnSortOrder[i]) {
          // Descending
          if (columnNullOrder != null && columnNullOrder.charAt(i) == 'a') {
            // Null first
            columnNotNullMarker[i] = BinarySortableSerDe.ZERO;
          } else {
            // Null last (default for descending order)
            columnNotNullMarker[i] = BinarySortableSerDe.ONE;
          }
        } else {
          // Ascending
          if (columnNullOrder != null && columnNullOrder.charAt(i) == 'z') {
            // Null last
            columnNotNullMarker[i] = BinarySortableSerDe.ZERO;
          } else {
            // Null first (default for ascending order)
            columnNotNullMarker[i] = BinarySortableSerDe.ONE;
          }
        }
    }
    return columnNotNullMarker;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    VectorExpression.doTransientInit(reduceSinkKeyExpressions, hconf);
    VectorExpression.doTransientInit(reduceSinkValueExpressions, hconf);

    if (LOG.isDebugEnabled()) {
      // Determine the name of our map or reduce task for debug tracing.
      BaseWork work = Utilities.getMapWork(hconf);
      if (work == null) {
        work = Utilities.getReduceWork(hconf);
      }
      taskName = work.getName();
    }

    reduceSkipTag = conf.getSkipTag();
    reduceTagByte = (byte) conf.getTag();

    if (LOG.isDebugEnabled()) { LOG.debug("Using tag = " + reduceTagByte); }
    numRows = 0;
    cntr = 1;

    if (!isEmptyKey) {
      keyBinarySortableSerializeWrite = BinarySortableSerializeWrite.with(
              conf.getKeySerializeInfo().getProperties(), reduceSinkKeyColumnMap.length);
    }

    if (!isEmptyValue) {
      valueLazyBinarySerializeWrite = new LazyBinarySerializeWrite(reduceSinkValueColumnMap.length);

      valueVectorSerializeRow =
          new VectorSerializeRow<LazyBinarySerializeWrite>(
              valueLazyBinarySerializeWrite);
      valueVectorSerializeRow.init(reduceSinkValueTypeInfos, reduceSinkValueColumnMap);

      valueOutput = new Output();
      valueVectorSerializeRow.setOutput(valueOutput);
    }

    keyWritable = new HiveKey();

    valueBytesWritable = new BytesWritable();

    int limit = conf.getTopN();
    float memUsage = conf.getTopNMemoryUsage();

    if (limit >= 0 && memUsage > 0) {
      reducerHash = new TopNHash();
      reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this, conf, hconf);
    }

    batchCounter = 0;

    vectorShuffleBatchAllowed = conf.isVectorShuffleBatchEnabled() && reducerHash == null;

    batchKeyHashCodes = null;
    batchValueHashCodes = null;
    vectorShuffleBatchSerializer = null;
    vectorShuffleSerializeBuffer = null;
    vectorShuffleBatchColumnMap = null;
    vectorShuffleBatchKey = null;
    vectorShuffleRowKeySerializeWrite = null;
    vectorShuffleRowKeyOutput = null;
    vectorShuffleRowKeySerializeRow = null;
    vectorShufflePartitionCounts = null;
    vectorShufflePartitionOffsets = null;
    vectorShufflePartitionPositions = null;
    vectorShuffleRowPartitions = null;
    vectorShufflePartitionRowIndices = null;
    vectorShufflePendingBuffers = null;
    vectorShufflePendingBytes = null;
    vectorShufflePendingRows = null;
    vectorShuffleNumUnorderedPartitions = -1;
    vectorShufflePartitionerType = -1;
  }

  protected boolean tryCollectVectorShuffleBatch(VectorizedRowBatch batch, int tag)
      throws HiveException, IOException {
    if (!vectorShuffleBatchAllowed || out == null) {
      return false;
    }

    if (vectorShuffleNumUnorderedPartitions == -1) {  // if this is the first call
      vectorShuffleNumUnorderedPartitions = out.getNumUnorderedPartitions();
      if (vectorShuffleNumUnorderedPartitions < 1) {
        vectorShuffleBatchAllowed = false;  // we never check vectorShuffleNumUnorderedPartitions again
        return false;
      }
      initializeVectorShuffleOutputScratch();
    }

    // Expressions evaluated by the concrete reduce-sink operator can filter every row after its
    // initial empty-batch check. Treat that batch as handled without emitting an empty shuffle
    // record, matching the per-row serialization path.
    if (batch.size == 0) {
      return true;
    }

    if (vectorShuffleNumUnorderedPartitions == 1) {
      // VectorizedRowBatch.size is the number of active logical rows. When
      // selectedInUse is true, selected[0..size) contains their physical indices.
      final int logicalRecordCount = batch.size;
      if (logicalRecordCount <= NUM_ROWS_THRESHOLD_FOR_BATCH) {
        collectVectorShuffleRows(batch, null, 0, logicalRecordCount, 0, tag);
      } else {
        while (true) {
          try {
            serializeVectorShuffleBatch(batch);
            break;
          } catch (ArrayIndexOutOfBoundsException ex) {
            growVectorShuffleSerializeBuffer();
          }
        }
        doCollectBatch(vectorShuffleBatchKey, valueBytesWritable, 0, logicalRecordCount);
      }
      return true;
    }

    collectPartitionedVectorShuffleBatch(batch, vectorShuffleNumUnorderedPartitions, tag);
    return true;
  }

  private void collectPartitionedVectorShuffleBatch(VectorizedRowBatch batch,
      int numUnorderedPartitions, int tag) throws HiveException, IOException {
    final int[] hashCodes;
    final int partitionerType = vectorShufflePartitionerType;
    if (partitionerType == 0) {
      hashCodes = batchKeyHashCodes;
      computeKeyHashCodes(batch, hashCodes);
    } else {
      hashCodes = batchValueHashCodes;
      computeValueHashCodes(batch, hashCodes);
    }

    Arrays.fill(vectorShufflePartitionCounts, 0, numUnorderedPartitions, 0);
    final boolean selectedInUse = batch.selectedInUse;
    final int[] selected = batch.selected;
    final int size = batch.size;
    for (int logical = 0; logical < size; logical++) {
      final int batchIndex = selectedInUse ? selected[logical] : logical;
      final int partition = (hashCodes[batchIndex] & Integer.MAX_VALUE) % numUnorderedPartitions;
      vectorShuffleRowPartitions[logical] = partition;
      vectorShufflePartitionCounts[partition]++;
    }

    vectorShufflePartitionOffsets[0] = 0;
    for (int partition = 0; partition < numUnorderedPartitions; partition++) {
      vectorShufflePartitionOffsets[partition + 1] =
          vectorShufflePartitionOffsets[partition] + vectorShufflePartitionCounts[partition];
      vectorShufflePartitionPositions[partition] = vectorShufflePartitionOffsets[partition];
    }

    for (int logical = 0; logical < size; logical++) {
      final int batchIndex = selectedInUse ? selected[logical] : logical;
      final int partition = vectorShuffleRowPartitions[logical];
      vectorShufflePartitionRowIndices[vectorShufflePartitionPositions[partition]++] = batchIndex;
    }

    for (int partition = 0; partition < numUnorderedPartitions; partition++) {
      final int rowCount = vectorShufflePartitionCounts[partition];
      if (rowCount == 0) {
        continue;
      }
      if (rowCount > VECTOR_SHUFFLE_ACCUMULATE_ROW_THRESHOLD) {
        collectVectorShuffleBatchWithPartition(batch, vectorShufflePartitionRowIndices,
            vectorShufflePartitionOffsets[partition], rowCount, partition);
      } else {
        appendVectorShuffleSegment(batch, vectorShufflePartitionRowIndices,
            vectorShufflePartitionOffsets[partition], rowCount, partition, tag);
      }
    }
  }

  private void initializeVectorShuffleOutputScratch() throws HiveException {
    assert vectorShuffleBatchSerializer == null;
    assert vectorShuffleNumUnorderedPartitions >= 1;

    vectorShuffleBatchSerializer = new VectorShuffleBatchSerializer();
    if (vectorShuffleNumUnorderedPartitions == 1) {
      vectorShuffleSerializeBuffer = new byte[SERIALIZE_BUFFER_SIZE];
    }

    final int keyColumnCount = isEmptyKey ? 0 : reduceSinkKeyColumnMap.length;
    final int valueColumnCount = isEmptyValue ? 0 : reduceSinkValueColumnMap.length;
    vectorShuffleBatchColumnMap = new int[keyColumnCount + valueColumnCount];
    if (keyColumnCount > 0) {
      System.arraycopy(reduceSinkKeyColumnMap, 0, vectorShuffleBatchColumnMap, 0, keyColumnCount);
    }
    if (valueColumnCount > 0) {
      System.arraycopy(reduceSinkValueColumnMap, 0, vectorShuffleBatchColumnMap, keyColumnCount,
          valueColumnCount);
    }

    vectorShuffleBatchKey = new HiveKey(ReduceRecordSource.VECTOR_BATCH_KEY_BYTES, 0);
    vectorShuffleBatchKey.setDistKeyLength(ReduceRecordSource.VECTOR_BATCH_KEY_BYTES.length);

    if (!isEmptyKey) {
      vectorShuffleRowKeySerializeWrite = BinarySortableSerializeWrite.with(
          conf.getKeySerializeInfo().getProperties(), reduceSinkKeyColumnMap.length);
      vectorShuffleRowKeyOutput = new Output();
      vectorShuffleRowKeySerializeWrite.set(vectorShuffleRowKeyOutput);
      vectorShuffleRowKeySerializeRow = new VectorSerializeRow<>(vectorShuffleRowKeySerializeWrite);
      vectorShuffleRowKeySerializeRow.init(reduceSinkKeyTypeInfos, reduceSinkKeyColumnMap);
    }

    if (vectorShuffleNumUnorderedPartitions > 1) {
      vectorShufflePartitionerType = out.getPartitionerType();

      vectorShufflePartitionCounts = new int[vectorShuffleNumUnorderedPartitions];
      vectorShufflePartitionOffsets = new int[vectorShuffleNumUnorderedPartitions + 1];
      vectorShufflePartitionPositions = new int[vectorShuffleNumUnorderedPartitions];
      vectorShufflePendingBuffers = new byte[vectorShuffleNumUnorderedPartitions][];
      vectorShufflePendingBytes = new int[vectorShuffleNumUnorderedPartitions];
      vectorShufflePendingRows = new long[vectorShuffleNumUnorderedPartitions];
      for (int partition = 0; partition < vectorShuffleNumUnorderedPartitions; partition++) {
        vectorShufflePendingBuffers[partition] = new byte[SERIALIZE_BUFFER_SIZE / 2];
        VectorShuffleBatchSerializer.writeInt(vectorShufflePendingBuffers[partition], 0,
            vectorShuffleBatchColumnMap.length);
        vectorShufflePendingBytes[partition] = Integer.BYTES;
      }
    }

    vectorShuffleRowPartitions = new int[VectorizedRowBatch.DEFAULT_SIZE];
    vectorShufflePartitionRowIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    if (vectorShuffleNumUnorderedPartitions > 1) {
      if (vectorShufflePartitionerType == 0) {
        batchKeyHashCodes = new int[VectorizedRowBatch.DEFAULT_SIZE];
      } else {
        batchValueHashCodes = new int[VectorizedRowBatch.DEFAULT_SIZE];
      }
    }
  }

  private void serializeVectorShuffleBatch(VectorizedRowBatch batch) {
    int length = vectorShuffleBatchSerializer.serialize(batch, vectorShuffleBatchColumnMap,
        vectorShuffleSerializeBuffer, 0);
    valueBytesWritable.set(vectorShuffleSerializeBuffer, 0, length);
  }

  private void collectVectorShuffleBatchWithPartition(VectorizedRowBatch batch, int[] rowIndices,
      int rowOffset, int rowCount, int partition) throws HiveException, IOException {
    byte[] serializeBuffer = new byte[SERIALIZE_BUFFER_SIZE];
    while (true) {
      try {
        int length = vectorShuffleBatchSerializer.serialize(batch, vectorShuffleBatchColumnMap,
            rowIndices, rowOffset, rowCount, serializeBuffer, 0);
        valueBytesWritable.set(serializeBuffer, 0, length);
        break;
      } catch (ArrayIndexOutOfBoundsException ex) {
        serializeBuffer = growVectorShuffleBuffer(serializeBuffer,
            "Vector shuffle direct serialize buffer size overflow");
      }
    }
    doCollectBatch(vectorShuffleBatchKey, valueBytesWritable, partition, rowCount);
  }

  private void appendVectorShuffleSegment(VectorizedRowBatch batch, int[] rowIndices, int rowOffset,
      int rowCount, int partition, int tag) throws HiveException, IOException {
    while (true) {
      byte[] pendingBuffer = vectorShufflePendingBuffers[partition];
      int segmentLengthPosition = vectorShufflePendingBytes[partition];
      try {
        if (segmentLengthPosition == Integer.BYTES) {
          vectorShufflePendingBytes[partition] = vectorShuffleBatchSerializer.serialize(batch,
              vectorShuffleBatchColumnMap, rowIndices, rowOffset, rowCount, pendingBuffer, 0);
        } else {
          VectorShuffleBatchSerializer.writeInt(pendingBuffer, segmentLengthPosition, 0);
          int segmentStart = segmentLengthPosition + Integer.BYTES;
          int segmentLength = vectorShuffleBatchSerializer.serializeSegmentBody(batch,
              vectorShuffleBatchColumnMap, rowIndices, rowOffset, rowCount, pendingBuffer, segmentStart);
          VectorShuffleBatchSerializer.writeInt(pendingBuffer, segmentLengthPosition, segmentLength);
          vectorShufflePendingBytes[partition] = segmentStart + segmentLength;
        }
        vectorShufflePendingRows[partition] += rowCount;
        if (vectorShufflePendingRows[partition] > VECTOR_SHUFFLE_ACCUMULATE_ROW_THRESHOLD) {
          flushVectorShufflePartition(partition);
        }
        return;
      } catch (ArrayIndexOutOfBoundsException ex) {
        if (segmentLengthPosition == Integer.BYTES) {
          collectVectorShuffleRows(batch, rowIndices, rowOffset, rowCount, partition, tag);
          return;
        }
        flushVectorShufflePartition(partition);
      }
    }
  }

  private void flushVectorShufflePartition(int partition) throws IOException {
    if (vectorShufflePendingBytes == null || vectorShufflePendingBytes[partition] <= Integer.BYTES) {
      return;
    }
    valueBytesWritable.set(vectorShufflePendingBuffers[partition], 0,
        vectorShufflePendingBytes[partition]);
    doCollectBatch(vectorShuffleBatchKey, valueBytesWritable, partition,
        vectorShufflePendingRows[partition]);
    vectorShufflePendingBytes[partition] = Integer.BYTES;
    vectorShufflePendingRows[partition] = 0;
  }

  private void flushVectorShufflePendingPartitions() throws IOException {
    if (vectorShufflePendingBytes == null) {
      return;
    }
    for (int partition = 0; partition < vectorShufflePendingBytes.length; partition++) {
      flushVectorShufflePartition(partition);
    }
  }

  private void growVectorShuffleSerializeBuffer() throws HiveException {
    vectorShuffleSerializeBuffer = growVectorShuffleBuffer(vectorShuffleSerializeBuffer,
        "Vector shuffle serialize buffer size overflow");
  }

  private byte[] growVectorShuffleBuffer(byte[] buffer, String errorMessage) throws HiveException {
    if (buffer.length > Integer.MAX_VALUE - SERIALIZE_BUFFER_SIZE) {
      throw new HiveException(errorMessage + ": " + buffer.length + " + "
          + SERIALIZE_BUFFER_SIZE);
    }
    return Arrays.copyOf(buffer, buffer.length + SERIALIZE_BUFFER_SIZE);
  }

  private void collectVectorShuffleRows(VectorizedRowBatch batch, int[] rowIndices, int rowOffset,
      int rowCount, int partition, int tag) throws IOException {
    for (int logical = 0; logical < rowCount; logical++) {
      final int batchIndex = rowIndices == null
          ? (batch.selectedInUse ? batch.selected[logical] : logical)
          : rowIndices[rowOffset + logical];
      serializeVectorShuffleRow(batch, batchIndex, tag);
      doCollectRowWithPartition(keyWritable, valueBytesWritable, partition);
    }
  }

  private void serializeVectorShuffleRow(VectorizedRowBatch batch, int batchIndex, int tag) throws IOException {
    try {
      if (isEmptyKey) {
        initializeEmptyKey(tag);
      } else {
        vectorShuffleRowKeySerializeWrite.reset();
        vectorShuffleRowKeySerializeRow.serializeWrite(batch, batchIndex);

        final int keyLength = vectorShuffleRowKeyOutput.getLength();
        setVectorShuffleRowKey(vectorShuffleRowKeyOutput.getData(), 0, keyLength,
            Murmur3.hash32(vectorShuffleRowKeyOutput.getData(), 0, keyLength, 0), tag);
      }

      if (isEmptyValue) {
        valueBytesWritable.setSize(0);
      } else {
        valueLazyBinarySerializeWrite.reset();
        valueVectorSerializeRow.serializeWrite(batch, batchIndex);
        valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());
      }
    } catch (Exception e) {
      throw new IOException("Failed to serialize vector shuffle row", e);
    }
  }

  protected void setVectorShuffleRowKey(byte[] keyBytes, int keyStart, int keyLength,
      int keyHashCode, int tag) {
    if (tag == -1 || reduceSkipTag) {
      keyWritable.set(keyBytes, keyStart, keyLength);
    } else {
      keyWritable.setSize(keyLength + 1);
      System.arraycopy(keyBytes, keyStart, keyWritable.get(), 0, keyLength);
      keyWritable.get()[keyLength] = reduceTagByte;
    }
    keyWritable.setDistKeyLength(keyLength);
    keyWritable.setHashCode(keyHashCode);
  }

  private void doCollectRowWithPartition(HiveKey keyWritable, BytesWritable valueWritable, int partition)
      throws IOException {
    if (null != out) {
      numRows++;
      if (LOG.isDebugEnabled()) {
        if (numRows >= cntr) {
          cntr = cntr * 10;
          LOG.debug("{}: records written - {}", this, numRows);
        }
      }
      out.writeWithPartition(keyWritable, valueWritable, partition);
    }
  }

  /**
   * Computes reducer-routing hash codes for all active rows in the batch.
   *
   * The result is indexed by physical batch row number. When batch.selectedInUse is true,
   * only entries for batch.selected[0..batch.size) are required to be valid.
   */
  protected abstract void computeKeyHashCodes(VectorizedRowBatch batch, int[] hashCodes)
      throws HiveException, IOException;

  /**
   * Computes reducer-routing value hash codes for all active rows in the batch.
   *
   * The hash is computed from the same serialized LazyBinary value representation that the
   * row-wise reduce-sink path writes to the edge. The result is indexed by physical batch row
   * number. When batch.selectedInUse is true, only entries for batch.selected[0..batch.size)
   * are required to be valid.
   */
  protected void computeValueHashCodes(VectorizedRowBatch batch, int[] hashCodes)
      throws HiveException {
    final boolean selectedInUse = batch.selectedInUse;
    final int[] selected = batch.selected;
    final int size = batch.size;

    if (isEmptyValue) {
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = selectedInUse ? selected[logical] : logical;
        hashCodes[batchIndex] = 0;
      }
      return;
    }

    try {
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = selectedInUse ? selected[logical] : logical;
        valueLazyBinarySerializeWrite.reset();
        valueVectorSerializeRow.serializeWrite(batch, batchIndex);
        valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());
        hashCodes[batchIndex] = valueBytesWritable.hashCode();
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected void initializeEmptyKey(int tag) {
    // Use the same logic as ReduceSinkOperator.toHiveKey.
    if (tag == -1 || reduceSkipTag) {
      keyWritable.setSize(0);
    } else {
      keyWritable.setSize(1);
      keyWritable.get()[0] = reduceTagByte;
    }
    keyWritable.setDistKeyLength(0);
    keyWritable.setHashCode(0);
  }

  // The collect method override for TopNHash.BinaryCollector
  @Override
  public void collect(byte[] key, byte[] value, int hash) throws IOException {
    HiveKey keyWritable = new HiveKey(key, hash);
    BytesWritable valueWritable = new BytesWritable(value);
    doCollect(keyWritable, valueWritable);
  }

  protected void collect(HiveKey keyWritable, BytesWritable valueWritable)
      throws HiveException, IOException {
    if (reducerHash != null) {
      // NOTE: partColsIsNull is only used for PTF, which isn't supported yet.
      final int firstIndex =
          reducerHash.tryStoreKey(keyWritable, /* partColsIsNull */ false);

      if (firstIndex == TopNHash.EXCLUDE) {
        return; // Nothing to do.
      }

      if (firstIndex == TopNHash.FORWARD) {
        doCollect(keyWritable, valueWritable);
      } else {
        Preconditions.checkState(firstIndex >= 0);
        reducerHash.storeValue(firstIndex, keyWritable.hashCode(), valueWritable, false);
      }
    } else {
      doCollect(keyWritable, valueWritable);
    }
  }

  private void doCollect(HiveKey keyWritable, BytesWritable valueWritable) throws IOException {
    // Since this is a terminal operator, update counters explicitly -
    // forward is not called
    if (null != out) {
      numRows++;
      if (LOG.isDebugEnabled()) {
        if (numRows >= cntr) {
          cntr = cntr * 10;
          LOG.info("{}: records written - {}", this, numRows);
        }
      }

      // BytesWritable valueBytesWritable = (BytesWritable) valueWritable;
      // LOG.info("VectorReduceSinkCommonOperator collect keyWritable " + keyWritable.getLength() + " " +
      //     VectorizedBatchUtil.displayBytes(keyWritable.getBytes(), 0, keyWritable.getLength()) +
      //     " valueWritable " + valueBytesWritable.getLength() +
      //     VectorizedBatchUtil.displayBytes(valueBytesWritable.getBytes(), 0, valueBytesWritable.getLength()));

      out.collect(keyWritable, valueWritable);
    }
  }

  private void doCollectBatch(HiveKey keyWritable, BytesWritable valueWritable, int partition,
                              long logicalRecordCount) throws IOException {
    if (null != out) {
      numRows += logicalRecordCount;
      if (LOG.isDebugEnabled()) {
        if (numRows >= cntr) {
          cntr = cntr * 10;
          LOG.debug("{}:: records written - {}", this, numRows);
        }
      }
      out.writeWithPartition(keyWritable, valueWritable, partition);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      try {
        flushVectorShufflePendingPartitions();
      } catch (IOException e) {
        throw new HiveException("Unable to flush vector shuffle pending partitions", e);
      }
    }
    if (!abort && reducerHash != null) {
      reducerHash.flush();
    }
    super.closeOp(abort);
    out = null;
    reducerHash = null;
    LOG.info("{}::: records written - {}", this, numRows);
    this.runTimeNumRows = numRows;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "RS";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.REDUCESINK;
  }

  @Override
  public VectorizationContext getOutputVectorizationContext() {
    return vContext;
  }

  @Override
  public boolean getIsReduceSink() {
    return true;
  }

  @Override
  public String getReduceOutputName() {
    return conf.getOutputName();
  }

  @Override
  public void setOutputCollector(OutputCollector _out) {
    this.out = (TezProcessor.TezKVOutputCollector) _out;
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}
