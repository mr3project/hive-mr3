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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchAccumulatingDeserializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchHeader;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchMetadata;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkCommonOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.io.BytesWritable;

/**
 * Owns the pending unordered shuffle batch and its unchecked decoder.
 * The legacy ReduceRecordSource path never touches this state or decoder.
 */
final class VectorShuffleBatchAccumulator {

  private final VectorizedRowBatchCtx batchContext;
  private final Operator<?> reducer;
  private final int inputColumnCount;
  private final byte tag;

  @FunctionalInterface
  interface RowDeserializerFactory {
    RowDeserializers create() throws HiveException;
  }

  static final class RowDeserializers {
    final VectorDeserializeRow<BinarySortableDeserializeRead> key;
    final VectorDeserializeRow<LazyBinaryDeserializeRead> value;

    RowDeserializers(VectorDeserializeRow<BinarySortableDeserializeRead> key,
        VectorDeserializeRow<LazyBinaryDeserializeRead> value) {
      this.key = key;
      this.value = value;
    }
  }

  private final RowDeserializerFactory rowDeserializerFactory;
  private RowDeserializers rowDeserializers;

  private final VectorShuffleBatchAccumulatingDeserializer decoder;
  private final int accumulatorRowLimit;

  private final boolean inputHasTypeMetadata;
  private final VectorShuffleBatchMetadata batchMetadata;

  private final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;

  private VectorizedRowBatch batch;

  private int[] accumulatorProjection;
  private int[] accumulatorSelection;
  private int pendingRows;
  private long pendingBytes;
  private boolean pendingCompact;

  VectorShuffleBatchAccumulator(VectorizedRowBatchCtx batchContext, Operator<?> reducer,
      VectorizedRowBatch batch, int inputColumnCount, byte tag,
      RowDeserializerFactory rowDeserializerFactory) {
    this.batchContext = batchContext;
    this.reducer = reducer;
    this.inputColumnCount = inputColumnCount;
    this.tag = tag;

    this.rowDeserializerFactory = rowDeserializerFactory;

    this.decoder = new VectorShuffleBatchAccumulatingDeserializer(batch, inputColumnCount);
    this.accumulatorRowLimit = decoder.getAppendCapacity();
    assert accumulatorRowLimit > 0;

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

  void process(BytesWritable keyWritable, BytesWritable valueWritable,
      boolean compact) throws HiveException, IOException {
    try {
      processRecord(keyWritable, valueWritable, compact);
    } finally {
      // the shuffle reader may reuse the incoming buffer as soon as this callback returns
      decoder.releaseHeader();
    }
  }

  private void processRecord(BytesWritable keyWritable, BytesWritable valueWritable,
      boolean compact) throws HiveException, IOException {
    assert batch.size == pendingRows;

    final VectorShuffleBatchHeader header = compact ? decoder.readHeader(valueWritable) : null;
    final int incomingRows = compact ? header.getRowCount() : 1;
    assert !compact || incomingRows > VectorReduceSinkCommonOperator.NUM_ROWS_THRESHOLD_FOR_BATCH;

    // use only the fixed header for the decision
    if (compact && (incomingRows >= accumulatorRowLimit || header.hasNestedRepeating())) {
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

    if (compact) {
      decoder.append(valueWritable, batch);
    } else {
      if (rowDeserializers == null) {
        rowDeserializers = rowDeserializerFactory.create();
      }
      rowDeserializers.key.setBytes(
          keyWritable.getBytesRaw(), keyWritable.getOffset(), keyWritable.getLength());
      rowDeserializers.key.deserialize(batch, pendingRows);
      if (rowDeserializers.value != null) {
        rowDeserializers.value.setBytes(
            valueWritable.getBytesRaw(), valueWritable.getOffset(), valueWritable.getLength());
        rowDeserializers.value.deserialize(batch, pendingRows);
      }
      batch.size = pendingRows + 1;
    }

    pendingRows += incomingRows;
    pendingBytes += incomingBytes;

    if (pendingRows == accumulatorRowLimit || pendingBytes >= BATCH_BYTES) {
      flushPendingBatch();
    }
  }

  void flushPendingBatch() throws HiveException {
    if (pendingRows == 0) {   // e.g., unconditional final flush
      return;
    }
    assert batch.size == pendingRows;
    final boolean retained = reducer.batchNeedsClone();
    reducer.process(batch, tag);
    // Never reset a batch retained by downstream code.
    // Allocate once per dispatch instead of once per incoming record.
    // The counters must not depend on the returned batch.size.
    prepareAccumulator(retained);
  }

  void discardPendingBatch() {
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
    initializeAccumulatorState();
  }

  // A newly created batch has already been reset by VectorizedRowBatchCtx.
  private void initializeAccumulatorState() {
    batch.projectedColumns = accumulatorProjection;
    batch.selected = accumulatorSelection;
    batch.projectionSize = inputColumnCount;
    for (int i = 0; i < batch.projectedColumns.length; i++) {
      batch.projectedColumns[i] = i;
    }
    discardPendingBatch();
  }

}
