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
 * Owns staged ordinary rows, separate ordinary and compact shuffle batches, and the compact decoder.
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

  private VectorShuffleBatchAccumulatingDeserializer decoder;
  private final int accumulatorRowLimit;
  private final int stagingRowLimit;

  // The shared layout resolves schema paths against each batch's independent vectors.
  private VectorShuffleBatchMetadata batchMetadata;

  private final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;

  private static final class BatchState {
    private VectorizedRowBatch batch;
    private int[] projection;
    private int[] selection;
    private int pendingRows;
    private long pendingBytes;

    private BatchState(VectorizedRowBatch batch, int inputColumnCount) {
      this.batch = batch;
      this.projection = batch.projectedColumns;
      this.selection = batch.selected;
      batch.projectionSize = inputColumnCount;
    }

    private void clearPending() {
      pendingRows = 0;
      pendingBytes = 0;
    }
  }

  private VectorizedRowBatch initialBatch;
  private BatchState compactState;
  private BatchState ordinaryState;

  private byte[] stagedBytes;
  private int[] stagedKeyLengths;
  private int[] stagedValueOffsets;
  private int[] stagedValueLengths;
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

    this.initialBatch = batch;
    this.accumulatorRowLimit = Math.min(VectorizedRowBatch.DEFAULT_SIZE, batch.getMaxSize());
    this.stagingRowLimit = Math.min(STAGING_CAPACITY, accumulatorRowLimit);
  }

  private BatchState createBatchState() {
    final VectorizedRowBatch batch;
    if (initialBatch != null) {
      batch = initialBatch;
      batchMetadata = new VectorShuffleBatchMetadata(batch.cols, batch.cols.length);
      initialBatch = null;
    } else {
      batch = batchContext.createVectorizedRowBatch();
    }
    assert Math.min(VectorizedRowBatch.DEFAULT_SIZE, batch.getMaxSize()) == accumulatorRowLimit;
    return new BatchState(batch, inputColumnCount);
  }

  private void ensureCompactState() {
    if (compactState == null) {
      final BatchState state = createBatchState();
      decoder = new VectorShuffleBatchAccumulatingDeserializer(state.batch, inputColumnCount);
      compactState = state;
      assert decoder.getAppendCapacity() == accumulatorRowLimit;
    }
  }

  public void process(BytesWritable keyWritable, BytesWritable valueWritable,
      boolean compact) throws HiveException, IOException {
    if (compact) {
      ensureCompactState();
      try {
        processCompact(keyWritable, valueWritable);
      } finally {
        // The compact decoder must not retain the incoming frame after this callback.
        decoder.releaseHeader();
      }
    } else {
      stageRow(keyWritable, valueWritable);
    }
  }

  private void stageRow(BytesWritable keyWritable, BytesWritable valueWritable)
      throws HiveException, IOException {
    ensureRowDeserializers();
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
      assert ordinaryState.pendingRows + stagedRows < accumulatorRowLimit;
      assert stagedRows == 0 || stagedBytes == keyBytes;

      // Reserve capacity for both decoded and staged ordinary rows before borrowing selection.
      final long incomingBytes = (long) keyWritable.getLength() + valueWritable.getLength();
      if (ordinaryState.pendingRows + stagedRows > 0
          && incomingBytes > BATCH_BYTES - ordinaryState.pendingBytes - stagedSerializedBytes) {
        consumeStagedRows();
        prepareOrdinaryAppend(1, incomingBytes);
      }

      final int[] stagedKeyOffsets = ordinaryState.selection;
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
      if (ordinaryState.pendingRows + stagedRows == accumulatorRowLimit
          || ordinaryState.pendingBytes + stagedSerializedBytes >= BATCH_BYTES) {
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
        assert ordinaryState.batch.size == ordinaryState.pendingRows;
        assert rowCount <= accumulatorRowLimit - ordinaryState.pendingRows;
        assert ordinaryState.pendingRows + rowCount == 1
            || incomingBytes <= BATCH_BYTES - ordinaryState.pendingBytes;
        final int[] stagedKeyOffsets = ordinaryState.selection;
        final int firstRow = ordinaryState.pendingRows;
        for (int i = 0; i < rowCount; i++) {
          deserializeRowAt(stagedBytes, stagedKeyOffsets[i], stagedKeyLengths[i],
              stagedBytes, stagedValueOffsets[i], stagedValueLengths[i], firstRow + i);
        }
      }
    } finally {
      clearStagedRows();
      releaseRowBytes();
    }
    if (rowCount > 0) {
      finishOrdinaryAppend(rowCount, incomingBytes);
    }
  }

  private void clearStagedRows() {
    stagedBytes = null;
    stagedRows = 0;
    stagedSerializedBytes = 0;
  }

  private void processCompact(BytesWritable keyWritable, BytesWritable valueWritable)
      throws HiveException, IOException {
    assert compactState.batch.size == compactState.pendingRows;

    final VectorShuffleBatchHeader header = decoder.readHeader(valueWritable);
    final int incomingRows = header.getRowCount();
    assert incomingRows > VectorReduceSinkCommonOperator.NUM_ROWS_THRESHOLD_FOR_BATCH;

    // use only the fixed header for the decision
    if (incomingRows >= accumulatorRowLimit || header.hasNestedRepeating()) {
      flushCompactBatch();
      // Full frames and frames with nested repetition retain their standalone representation.
      decoder.deserializeWithHeader(valueWritable, compactState.batch);
      compactState.pendingRows = incomingRows;
      flushCompactBatch();
      return;
    }

    final long incomingBytes = (long) keyWritable.getLength() + valueWritable.getLength();
    if (compactState.pendingRows > 0 && (
         incomingRows > accumulatorRowLimit - compactState.pendingRows
      || incomingBytes > BATCH_BYTES - compactState.pendingBytes
      || !decoder.isCompatible())) {
      flushCompactBatch();
    }
    decoder.append(valueWritable, compactState.batch);
    compactState.pendingRows += incomingRows;
    compactState.pendingBytes += incomingBytes;
    assert compactState.batch.size == compactState.pendingRows;

    if (compactState.pendingRows == accumulatorRowLimit || compactState.pendingBytes >= BATCH_BYTES) {
      flushCompactBatch();
    }
  }

  private void prepareOrdinaryAppend(int incomingRows, long incomingBytes)
      throws HiveException {
    assert stagedRows == 0;
    assert ordinaryState.batch.size == ordinaryState.pendingRows;
    if (ordinaryState.pendingRows > 0 && (
         incomingRows > accumulatorRowLimit - ordinaryState.pendingRows
      || incomingBytes > BATCH_BYTES - ordinaryState.pendingBytes)) {
      flushOrdinaryBatch();
    }
  }

  private void deserializeRow(byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength) throws HiveException, IOException {
    final long incomingBytes = (long) keyLength + valueLength;
    prepareOrdinaryAppend(1, incomingBytes);
    deserializeRowAt(keyBytes, keyOffset, keyLength,
        valueBytes, valueOffset, valueLength, ordinaryState.pendingRows);
    finishOrdinaryAppend(1, incomingBytes);
  }

  private void ensureRowDeserializers() throws HiveException {
    if (rowDeserializers == null) {
      ordinaryState = createBatchState();
      stagedKeyLengths = new int[STAGING_CAPACITY];
      stagedValueOffsets = new int[STAGING_CAPACITY];
      stagedValueLengths = new int[STAGING_CAPACITY];
      rowDeserializers = rowDeserializerFactory.create();
    }
  }

  private void deserializeRowAt(byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength, int batchIndex)
      throws HiveException, IOException {
    final VectorizedRowBatch batch = ordinaryState.batch;
    assert batchIndex == batch.size;
    assert rowDeserializers != null;
    rowDeserializers.key.setBytes(keyBytes, keyOffset, keyLength);
    rowDeserializers.key.deserialize(batch, batchIndex);
    if (rowDeserializers.value != null) {
      rowDeserializers.value.setBytes(valueBytes, valueOffset, valueLength);
      rowDeserializers.value.deserialize(batch, batchIndex);
    }
    batch.size = batchIndex + 1;
  }

  private void finishOrdinaryAppend(int incomingRows, long incomingBytes) throws HiveException {
    ordinaryState.pendingRows += incomingRows;
    ordinaryState.pendingBytes += incomingBytes;
    assert ordinaryState.batch.size == ordinaryState.pendingRows;

    if (ordinaryState.pendingRows == accumulatorRowLimit || ordinaryState.pendingBytes >= BATCH_BYTES) {
      flushOrdinaryBatch();
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
    flushPendingBatch();
  }

  public void flushPendingBatch() throws HiveException, IOException {
    consumeStagedRows();
    flushOrdinaryBatch();
    flushCompactBatch();
  }

  private void flushOrdinaryBatch() throws HiveException {
    assert stagedRows == 0;
    if (ordinaryState != null) {
      flushBatch(ordinaryState);
    }
  }

  private void flushCompactBatch() throws HiveException {
    if (compactState != null) {
      flushBatch(compactState);
      decoder.resetAccumulation();
    }
  }

  private void flushBatch(BatchState state) throws HiveException {
    if (state.pendingRows == 0) {   // e.g., unconditional final flush
      return;
    }
    assert state.batch.size == state.pendingRows;
    final boolean retained = reducer.batchNeedsClone();
    reducer.process(state.batch, tag);
    // Never reset a batch retained by downstream code.
    // The counters must not depend on the returned batch.size.
    prepareAccumulator(state, retained);
  }

  public void discardPendingBatch() {
    clearStagedRows();
    releaseRowBytes();
    if (ordinaryState != null) {
      ordinaryState.clearPending();
    }
    if (compactState != null) {
      decoder.releaseHeader();
      decoder.resetAccumulation();
      compactState.clearPending();
    }
  }

  private void prepareAccumulator(BatchState state, boolean replace) {
    if (replace) {
      state.batch = batchContext.createVectorizedRowBatch();
      state.projection = state.batch.projectedColumns;
      state.selection = state.batch.selected;
    } else {
      state.batch.reset();  // includes scratch columns, nested storage, selection, and EOF state
      batchMetadata.reset(state.batch.cols);
    }

    state.batch.projectedColumns = state.projection;
    state.batch.selected = state.selection;
    state.batch.projectionSize = inputColumnCount;
    for (int i = 0; i < state.batch.projectedColumns.length; i++) {
      state.batch.projectedColumns[i] = i;
    }
    state.clearPending();
  }
}
