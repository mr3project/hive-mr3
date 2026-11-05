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
 * Owns the pending unordered shuffle batch and its unchecked decoder.
 * The legacy ReduceRecordSource path never touches this state or decoder.
 */
public final class VectorShuffleBatchAccumulator {

  private final VectorizedRowBatchCtx batchContext;
  private final Operator<?> reducer;
  private final int inputColumnCount;
  private final byte tag;
  private final boolean byteReferencesAllowed;

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

  private final boolean inputHasTypeMetadata;
  private final VectorShuffleBatchMetadata batchMetadata;

  private final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;

  private VectorizedRowBatch batch;
  private ColumnVector[] accumulatorColumns;

  private int[] accumulatorProjection;
  private int[] accumulatorSelection;
  private int pendingRows;
  private long pendingBytes;
  private boolean pendingCompact;

  public VectorShuffleBatchAccumulator(VectorizedRowBatchCtx batchContext, Operator<?> reducer,
      VectorizedRowBatch batch, int inputColumnCount, byte tag,
      RowDeserializerFactory rowDeserializerFactory) {
    this(batchContext, reducer, batch, inputColumnCount, tag, rowDeserializerFactory, false);
  }

  public VectorShuffleBatchAccumulator(VectorizedRowBatchCtx batchContext, Operator<?> reducer,
      VectorizedRowBatch batch, int inputColumnCount, byte tag,
      RowDeserializerFactory rowDeserializerFactory, boolean byteReferencesAllowed) {
    this.batchContext = batchContext;
    this.reducer = reducer;
    this.inputColumnCount = inputColumnCount;
    this.tag = tag;
    this.byteReferencesAllowed = byteReferencesAllowed;

    this.rowDeserializerFactory = rowDeserializerFactory;

    this.decoder = new VectorShuffleBatchAccumulatingDeserializer(
        batch, inputColumnCount, byteReferencesAllowed);
    this.accumulatorRowLimit = decoder.getAppendCapacity();
    assert accumulatorRowLimit > 0;

    this.inputHasTypeMetadata = decoder.hasInputTypeMetadata();
    this.batchMetadata = new VectorShuffleBatchMetadata(batch.cols, batch.cols.length);

    this.batch = batch;
    this.accumulatorColumns = byteReferencesAllowed ? batch.cols.clone() : null;

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
      processRecord(keyWritable, valueWritable, compact);
    } finally {
      // The reader may reuse the wrapper; borrowed backing arrays remain immutable.
      decoder.releaseHeader();
      if (byteReferencesAllowed && !compact && rowDeserializers != null) {
        rowDeserializers.key.releaseInput();
        if (rowDeserializers.value != null) {
          rowDeserializers.value.releaseInput();
        }
      }
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

  public void flushPendingBatch() throws HiveException {
    if (pendingRows == 0) {   // e.g., unconditional final flush
      return;
    }
    assert batch.size == pendingRows;
    final boolean retained = reducer.batchNeedsClone();
    assert !byteReferencesAllowed || !retained;
    try {
      reducer.process(batch, tag);
    } finally {
      releaseByteReferences();
    }
    // Never reset a batch retained by downstream code.
    // The counters must not depend on the returned batch.size.
    prepareAccumulator(retained);
  }

  public void discardPendingBatch() {
    releaseByteReferences();
    resetPendingBatch();
  }

  private void resetPendingBatch() {
    decoder.resetAccumulation();
    pendingRows = 0;
    pendingBytes = 0;
    pendingCompact = false;
  }

  private void releaseByteReferences() {
    if (byteReferencesAllowed) {
      // Keep the original owners available even if an operator replaces a batch column.
      for (int i = 0; i < accumulatorColumns.length; i++) {
        VectorizedBatchUtil.clearByteReferences(accumulatorColumns[i]);
        if (batch.cols[i] != accumulatorColumns[i]) {
          VectorizedBatchUtil.clearByteReferences(batch.cols[i]);
        }
      }
    }
  }

  private void prepareAccumulator(boolean replace) {
    if (replace) {
      batch = batchContext.createVectorizedRowBatch();
      accumulatorColumns = byteReferencesAllowed ? batch.cols.clone() : null;
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
    resetPendingBatch();
  }
}
