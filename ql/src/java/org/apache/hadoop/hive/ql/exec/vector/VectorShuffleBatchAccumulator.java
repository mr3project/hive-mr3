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
package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkCommonOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.io.BytesWritable;

/**
 * Owns staged ordinary rows, the pending unordered shuffle batch, and its unchecked decoder.
 * The legacy ReduceRecordSource path never touches this state or decoder.
 */
public final class VectorShuffleBatchAccumulator {

  private static final int STAGING_CAPACITY = VectorizedRowBatch.DEFAULT_SIZE;

  private final VectorizedRowBatchCtx batchContext;
  private final Operator<?> reducer;
  private final int inputColumnCount;
  private final byte tag;

  /** Creates row-mode readers lazily when an ordinary shuffle record arrives. */
  @FunctionalInterface
  public interface RowDeserializerFactory {
    RowDeserializers create() throws HiveException;
  }

  /** Row-mode key reader and optional value reader. */
  public static final class RowDeserializers {
    public final VectorDeserializeRow<BinarySortableDeserializeRead> key;
    public final VectorDeserializeRow<LazyBinaryDeserializeRead> value;

    public RowDeserializers(VectorDeserializeRow<BinarySortableDeserializeRead> key,
        VectorDeserializeRow<LazyBinaryDeserializeRead> value) {
      this.key = key;
      this.value = value;
    }
  }

  private final RowDeserializerFactory rowDeserializerFactory;
  private RowDeserializers rowDeserializers;

  private final VectorShuffleBatchAccumulatingDeserializer decoder;
  private final int accumulatorRowLimit;
  private final int stagingRowLimit;

  private final boolean inputHasTypeMetadata;
  private final VectorShuffleBatchMetadata batchMetadata;

  private final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;

  private VectorizedRowBatch batch;

  private int[] accumulatorProjection;
  private int[] accumulatorSelection;
  private int pendingRows;
  private long pendingBytes;
  private boolean pendingCompact;

  private byte[] stagedBytes;
  private final int[] stagedKeyOffsets = new int[STAGING_CAPACITY];
  private final int[] stagedKeyLengths = new int[STAGING_CAPACITY];
  private final int[] stagedValueOffsets = new int[STAGING_CAPACITY];
  private final int[] stagedValueLengths = new int[STAGING_CAPACITY];
  private int stagedRows;
  private long stagedSerializedBytes;

  public VectorShuffleBatchAccumulator(VectorizedRowBatchCtx batchContext, Operator<?> reducer,
      VectorizedRowBatch batch, int inputColumnCount, byte tag,
      RowDeserializerFactory rowDeserializerFactory) {
    this.batchContext = batchContext;
    this.reducer = reducer;
    this.inputColumnCount = inputColumnCount;
    this.tag = tag;

    this.rowDeserializerFactory = rowDeserializerFactory;

    this.decoder = new VectorShuffleBatchAccumulatingDeserializer(batch, inputColumnCount);
    this.accumulatorRowLimit = decoder.getAppendCapacity();
    this.stagingRowLimit = Math.min(STAGING_CAPACITY, accumulatorRowLimit);

    this.inputHasTypeMetadata = decoder.hasInputTypeMetadata();
    this.batchMetadata = new VectorShuffleBatchMetadata(batch.cols, batch.cols.length);

    this.batch = batch;

    this.accumulatorProjection = batch.projectedColumns;
    this.accumulatorSelection = batch.selected;
    this.pendingRows = 0;
    this.pendingBytes = 0L;
    this.pendingCompact = false;

    batch.projectionSize = inputColumnCount;
  }

  public void process(BytesWritable keyWritable, BytesWritable valueWritable,
      boolean compact) throws HiveException, IOException {
    try {
      if (compact) {
        processCompact(keyWritable, valueWritable);
      } else {
        stageRow(keyWritable, valueWritable);
      }
    } finally {
      // The compact decoder must not retain the incoming frame after this callback.
      decoder.releaseHeader();
    }
  }

  private void stageRow(BytesWritable keyWritable, BytesWritable valueWritable)
      throws HiveException, IOException {
    byte[] keyBytes = keyWritable.getBytesRaw();
    byte[] valueBytes = valueWritable.getBytesRaw();
    if (keyBytes != valueBytes) {
      // records are from disk-backed IFile.Reader
      assert stagedRows == 0 && stagedBytes == null && stagedSerializedBytes == 0;
      deserializeRow(keyBytes, keyWritable.getOffset(), keyWritable.getLength(),
          valueBytes, valueWritable.getOffset(), valueWritable.getLength());
    } else {
      // records are from in-memory IFile.Reader
      assert stagedRows < stagingRowLimit;
      assert stagedRows == 0 || stagedBytes == keyBytes;

      // keep the staged group within one decoded batch's byte budget
      final long incomingBytes = (long) keyWritable.getLength() + valueWritable.getLength();
      if (stagedRows > 0 && incomingBytes + stagedSerializedBytes > BATCH_BYTES) {
        consumeStagedRows();
      }

      if (stagedRows == 0) {
        stagedBytes = keyBytes;
      }
      stagedKeyOffsets[stagedRows] = keyWritable.getOffset();
      stagedKeyLengths[stagedRows] = keyWritable.getLength();
      stagedValueOffsets[stagedRows] = valueWritable.getOffset();
      stagedValueLengths[stagedRows] = valueWritable.getLength();
      stagedRows++;
      stagedSerializedBytes += incomingBytes;

      // a single over-sized row is admitted alone and materialized immediately
      if (stagedRows == stagingRowLimit || stagedSerializedBytes >= BATCH_BYTES) {
        consumeStagedRows();
      }
    }
  }

  public void consumeStagedRows() throws HiveException, IOException {
    final int rowCount = stagedRows;
    final long incomingBytes = stagedSerializedBytes;

    assert rowCount <= stagingRowLimit;
    assert rowCount == 1 || incomingBytes <= BATCH_BYTES;

    try {
      if (rowCount > 0) {
        prepareForAppend(rowCount, incomingBytes, false);
        for (int i = 0; i < rowCount; i++) {
          deserializeRowAt(stagedBytes, stagedKeyOffsets[i], stagedKeyLengths[i],
              stagedBytes, stagedValueOffsets[i], stagedValueLengths[i], pendingRows + i);
        }
        finishAppend(rowCount, incomingBytes);
      }
    } finally {
      clearStagedRows();
      releaseRowBytes();
    }
  }

  private void clearStagedRows() {
    stagedBytes = null;
    stagedRows = 0;
    stagedSerializedBytes = 0;
  }

  private void processCompact(BytesWritable keyWritable, BytesWritable valueWritable)
      throws HiveException, IOException {
    assert batch.size == pendingRows;

    final VectorShuffleBatchHeader header = decoder.readHeader(valueWritable);
    final int incomingRows = header.getRowCount();
    assert incomingRows > VectorReduceSinkCommonOperator.NUM_ROWS_THRESHOLD_FOR_BATCH;

    // use only the fixed header for the decision
    if (incomingRows >= accumulatorRowLimit || header.hasNestedRepeating()) {
      if (pendingRows > 0) {
        flushPendingBatch();
      }
      // Full frames and frames with nested repetition retain their standalone representation.
      decoder.deserializeWithHeader(valueWritable, batch);
      pendingRows = incomingRows;
      pendingCompact = true;
      flushPendingBatch();
      return;
    }

    final long incomingBytes = (long) keyWritable.getLength() + valueWritable.getLength();
    prepareForAppend(incomingRows, incomingBytes, true);
    decoder.append(valueWritable, batch);
    finishAppend(incomingRows, incomingBytes);
  }

  private void prepareForAppend(int incomingRows, long incomingBytes, boolean compact)
      throws HiveException {
    assert batch.size == pendingRows;
    if (pendingRows == 0) {
      pendingCompact = compact;
    } else if (
         incomingRows > accumulatorRowLimit - pendingRows     // prevents exceeding the batch row capacity
      || incomingBytes > BATCH_BYTES - pendingBytes           // enforces the serialized-byte accumulation budget
      || (inputHasTypeMetadata && compact != pendingCompact)  // Prevents mixing row-mode and compact data with potentially incompatible date/timestamp metadata
      || (compact ?
           !decoder.isCompatible() :          // detects incompatible repeating values, bitmaps and metadata
           decoder.hasRepeatingColumns())) {  // prevents appending an ordinary row to a batch containing repeating columns
      flushPendingBatch();
      pendingCompact = compact;
    }
    assert pendingRows == 0 || !inputHasTypeMetadata || compact == pendingCompact;
  }

  private void deserializeRow(byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength) throws HiveException, IOException {
    final long incomingBytes = (long) keyLength + valueLength;
    prepareForAppend(1, incomingBytes, false);
    deserializeRowAt(keyBytes, keyOffset, keyLength, valueBytes, valueOffset, valueLength, pendingRows);
    finishAppend(1, incomingBytes);
  }

  private void deserializeRowAt(byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength, int batchIndex)
      throws HiveException, IOException {
    assert batchIndex == batch.size;
    if (rowDeserializers == null) {
      rowDeserializers = rowDeserializerFactory.create();
    }
    rowDeserializers.key.setBytes(keyBytes, keyOffset, keyLength);
    rowDeserializers.key.deserialize(batch, batchIndex);
    if (rowDeserializers.value != null) {
      rowDeserializers.value.setBytes(valueBytes, valueOffset, valueLength);
      rowDeserializers.value.deserialize(batch, batchIndex);
    }
    batch.size = batchIndex + 1;
  }

  private void finishAppend(int incomingRows, long incomingBytes) throws HiveException {
    pendingRows += incomingRows;
    pendingBytes += incomingBytes;

    if (pendingRows == accumulatorRowLimit || pendingBytes >= BATCH_BYTES) {
      flushPendingBatch();
    }
  }

  private void releaseRowBytes() {
    if (rowDeserializers != null) {
      rowDeserializers.key.releaseBytes();
      if (rowDeserializers.value != null) {
        rowDeserializers.value.releaseBytes();
      }
    }
  }

  public void finish() throws HiveException, IOException {
    consumeStagedRows();
    flushPendingBatch();
  }

  public void flushPendingBatch() throws HiveException {
    if (pendingRows == 0) {   // e.g., unconditional final flush
      return;
    }
    assert batch.size == pendingRows;
    final boolean retained = reducer.batchNeedsClone();
    reducer.process(batch, tag);
    // Never reset a batch retained by downstream code.
    // The counters must not depend on the returned batch.size.
    prepareAccumulator(retained);
  }

  public void discardPendingBatch() {
    clearStagedRows();
    releaseRowBytes();
    decoder.releaseHeader();
    resetDecodedBatch();
  }

  private void resetDecodedBatch() {
    decoder.resetAccumulation();
    pendingRows = 0;
    pendingBytes = 0;
    pendingCompact = false;
  }

  private void prepareAccumulator(boolean replace) {
    if (replace) {
      batch = batchContext.createVectorizedRowBatch();
      accumulatorProjection = batch.projectedColumns;
      accumulatorSelection = batch.selected;
    } else {
      batch.reset();  // includes scratch columns, nested storage, selection, and EOF state
      batchMetadata.reset(batch.cols);
    }

    batch.projectedColumns = accumulatorProjection;
    batch.selected = accumulatorSelection;
    batch.projectionSize = inputColumnCount;
    for (int i = 0; i < batch.projectedColumns.length; i++) {
      batch.projectedColumns[i] = i;
    }
    resetDecodedBatch();
  }
}
