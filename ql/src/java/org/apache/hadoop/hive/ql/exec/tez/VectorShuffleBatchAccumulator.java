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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchAccumulatingDeserializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
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
  private final byte tag;
  private final VectorDeserializeRow<BinarySortableDeserializeRead>
      keyBinarySortableDeserializeToRow;
  private final VectorDeserializeRow<LazyBinaryDeserializeRead>
      valueLazyBinaryDeserializeToRow;
  private final VectorShuffleBatchAccumulatingDeserializer decoder;
  private final int inputColumnCount;
  private final int accumulatorRowLimit;

  private final boolean inputHasTypeMetadata;
  private final boolean batchHasTypeMetadata;

  private final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;
  private VectorizedRowBatch batch;
  private int[] accumulatorProjection;
  private int[] accumulatorSelection;
  private int pendingRows;
  private long pendingBytes;
  private boolean pendingCompact;

  VectorShuffleBatchAccumulator(VectorizedRowBatchCtx batchContext, Operator<?> reducer,
      VectorizedRowBatch batch, int inputColumnCount, byte tag,
      VectorDeserializeRow<BinarySortableDeserializeRead> keyBinarySortableDeserializeToRow,
      VectorDeserializeRow<LazyBinaryDeserializeRead> valueLazyBinaryDeserializeToRow) {
    this.batchContext = batchContext;
    this.reducer = reducer;
    this.batch = batch;
    this.inputColumnCount = inputColumnCount;
    this.tag = tag;
    this.keyBinarySortableDeserializeToRow = keyBinarySortableDeserializeToRow;
    this.valueLazyBinaryDeserializeToRow = valueLazyBinaryDeserializeToRow;
    decoder = new VectorShuffleBatchAccumulatingDeserializer(batch, inputColumnCount);

    inputHasTypeMetadata = decoder.hasInputTypeMetadata();
    boolean hasMetadata = inputHasTypeMetadata;
    for (int i = inputColumnCount; i < batch.cols.length && !hasMetadata; i++) {
      hasMetadata = VectorShuffleBatchAccumulatingDeserializer.hasTypeMetadata(batch.cols[i]);
    }
    batchHasTypeMetadata = hasMetadata;

    accumulatorRowLimit = decoder.getAppendCapacity();
    assert accumulatorRowLimit > 0;
    accumulatorProjection = batch.projectedColumns;
    accumulatorSelection = batch.selected;
    initializeAccumulatorState();
  }

  void process(BytesWritable keyWritable, BytesWritable valueWritable,
      boolean compact) throws HiveException, IOException {
    assert !batch.selectedInUse && batch.size == pendingRows;
    final int incomingRows = compact
        ? VectorShuffleBatchAccumulatingDeserializer.getRowCount(valueWritable) : 1;
    if (incomingRows < 0) {
      throw new IOException("Negative vector shuffle batch row count " + incomingRows);
    }
    if (incomingRows == 0) {
      // Empty frames carry no group or EOF signal and do not consume the byte budget.
      return;
    }

    // Inspect before mutating the pending batch. Metadata/repetition conflicts flush the batch;
    // nested repeating vectors use standalone decoding so they are never expanded.
    final VectorShuffleBatchAccumulatingDeserializer.FrameInfo frameInfo =
        compact && incomingRows < accumulatorRowLimit
            ? decoder.inspectAccumulation(valueWritable, batch, incomingRows)
            : null;

    if (compact && (frameInfo == null || !frameInfo.isAppendCompatible())) {
      if (pendingRows > 0) {
        flushPendingBatch();
      }
      // Full frames and frames with nested repetition retain their standalone representation.
      decoder.deserialize(valueWritable, batch);
      pendingRows = incomingRows;
      pendingCompact = true;
      flushPendingBatch();
      return;
    }

    final long incomingBytes = (long) keyWritable.getLength() + valueWritable.getLength();
    if (pendingRows > 0 && (
           incomingRows > accumulatorRowLimit - pendingRows
        || incomingBytes > BATCH_BYTES - pendingBytes
        || (compact && !frameInfo.isRepeatingCompatible())  // TODO: redundant???
        || (!compact && decoder.hasRepeatingColumns())
        || (inputHasTypeMetadata && (
            compact != pendingCompact ||
            (compact && !frameInfo.isMetadataCompatible())))
        )) {
      flushPendingBatch();
    }

    // Rows carry no calendar/UTC flags, so keep their default metadata policy separate from frames.
    assert pendingRows == 0 || !inputHasTypeMetadata || compact == pendingCompact;
    if (pendingRows == 0) {
      pendingCompact = compact;
    }
    if (compact) {
      decoder.appendInspectedAccumulation(valueWritable, batch, incomingRows);
    } else {
      keyBinarySortableDeserializeToRow.setBytes(keyWritable.getBytesRaw(), keyWritable.getOffset(),
          keyWritable.getLength());
      keyBinarySortableDeserializeToRow.deserialize(batch, pendingRows);
      if (valueLazyBinaryDeserializeToRow != null) {
        valueLazyBinaryDeserializeToRow.setBytes(valueWritable.getBytesRaw(),
            valueWritable.getOffset(), valueWritable.getLength());
        valueLazyBinaryDeserializeToRow.deserialize(batch, pendingRows);
      }
      batch.size = pendingRows + 1;
    }

    pendingRows += incomingRows;
    pendingBytes += incomingBytes;

    // Serialized bytes are only a soft threshold. Repeating and nested values can materialize
    // into much more decoded memory than their wire size.
    if (pendingRows == accumulatorRowLimit || pendingBytes >= BATCH_BYTES) {
      flushPendingBatch();
    }
  }

  void flushPendingBatch() throws HiveException {
    if (pendingRows == 0) {
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
      if (batchHasTypeMetadata) {
        for (ColumnVector column : batch.cols) {
          resetTypeMetadataToDefaults(column);
        }
      }
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
    decoder.resetAccumulation();
    discardPendingBatch();
  }

  // Row decoding expects the same metadata as a fresh VectorizedRowBatchCtx batch.
  // Reset does not clear these flags, and compact decoding may have installed different sender metadata.
  private static void resetTypeMetadataToDefaults(ColumnVector column) {
    if (column instanceof DateColumnVector) {
      ((DateColumnVector) column).setUsingProlepticCalendar(false);
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      timestamp.setIsUTC(false);
      timestamp.setUsingProlepticCalendar(false);
    } else if (column instanceof ListColumnVector) {
      resetTypeMetadataToDefaults(((ListColumnVector) column).child);
    } else if (column instanceof MapColumnVector) {
      resetTypeMetadataToDefaults(((MapColumnVector) column).keys);
      resetTypeMetadataToDefaults(((MapColumnVector) column).values);
    } else if (column instanceof StructColumnVector) {
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        resetTypeMetadataToDefaults(field);
      }
    } else if (column instanceof UnionColumnVector) {
      for (ColumnVector field : ((UnionColumnVector) column).fields) {
        resetTypeMetadataToDefaults(field);
      }
    }
  }

}
