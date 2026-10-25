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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchAccumulator {
  private static final BytesWritable EMPTY = new BytesWritable();

  @Test
  public void testCompatibleRepeatingFramesAndBitmapChanges() throws Exception {
    Harness harness = new Harness(() -> batch(new LongColumnVector(), new LongColumnVector()),
        2, null);
    VectorizedRowBatch source = batch(new LongColumnVector(), new LongColumnVector());
    source.size = 2;
    LongColumnVector repeated = (LongColumnVector) source.cols[0];
    LongColumnVector ordinary = (LongColumnVector) source.cols[1];
    repeated.isRepeating = true;
    repeated.vector[0] = 7;
    ordinary.vector[0] = 10;
    ordinary.vector[1] = 11;
    harness.process(source);
    ordinary.vector[0] = 12;
    ordinary.vector[1] = 13;
    harness.process(source);
    assertTrue(harness.sizes.isEmpty());
    assertEquals(4, harness.initial.size);
    assertTrue(harness.initial.cols[0].isRepeating);
    assertArrayEquals(new long[] {10, 11, 12, 13},
        Arrays.copyOf(((LongColumnVector) harness.initial.cols[1]).vector, 4));

    repeated.vector[0] = 8;
    harness.process(source);
    assertEquals(Arrays.asList(4), harness.sizes);
    // Equal dense values must still change the bitmap and flush the repeating batch.
    repeated.isRepeating = false;
    repeated.vector[1] = 8;
    harness.process(source);
    assertEquals(Arrays.asList(4, 2), harness.sizes);
    assertFalse(harness.initial.cols[0].isRepeating);
    harness.accumulator.flushPendingBatch();
    assertEquals(Arrays.asList(4, 2, 2), harness.sizes);
  }

  @Test
  public void testEmptyNestedMetadataMismatchFlushesAndReuseRestoresDefaults() throws Exception {
    Harness harness = new Harness(
        () -> batch(new ListColumnVector(1024, new DateColumnVector())), 1, null);
    DateColumnVector child = new DateColumnVector().setUsingProlepticCalendar(true);
    VectorizedRowBatch source = batch(new ListColumnVector(1024, child));
    source.size = 2;
    harness.process(source);
    assertTrue(((DateColumnVector) ((ListColumnVector) harness.initial.cols[0]).child)
        .isUsingProlepticCalendar());
    child.setUsingProlepticCalendar(false);
    harness.process(source);
    assertEquals(Arrays.asList(2), harness.sizes);
    assertFalse(((DateColumnVector) ((ListColumnVector) harness.initial.cols[0]).child)
        .isUsingProlepticCalendar());
    child.setUsingProlepticCalendar(true);
    harness.process(source);
    harness.accumulator.flushPendingBatch();
    assertEquals(Arrays.asList(2, 2, 2), harness.sizes);
    assertFalse(((DateColumnVector) ((ListColumnVector) harness.initial.cols[0]).child)
        .isUsingProlepticCalendar());
  }

  @Test
  public void testOrdinaryRowFlushesRepeatingBatch() throws Exception {
    VectorDeserializeRow<BinarySortableDeserializeRead> rowReader =
        new VectorDeserializeRow<>(BinarySortableDeserializeRead.ascendingNullsFirst(
            new TypeInfo[] {TypeInfoFactory.longTypeInfo}, false));
    rowReader.init();
    Harness harness = new Harness(() -> batch(new LongColumnVector()), 1, rowReader);
    VectorizedRowBatch source = longs(2, 7);
    source.cols[0].isRepeating = true;
    harness.process(source);

    Output rowBytes = new Output();
    BinarySortableSerializeWrite writer = new BinarySortableSerializeWrite(1);
    writer.set(rowBytes);
    writer.writeLong(9);
    BytesWritable key = new BytesWritable();
    key.setDirect(rowBytes.getData(), 0, rowBytes.getLength());
    harness.accumulator.process(key, EMPTY, false);
    assertEquals(Arrays.asList(2), harness.sizes);
    assertEquals(1, harness.initial.size);
    assertFalse(harness.initial.cols[0].isRepeating);
    assertEquals(9, ((LongColumnVector) harness.initial.cols[0]).vector[0]);
    harness.accumulator.flushPendingBatch();
    assertEquals(Arrays.asList(2, 1), harness.sizes);
  }

  @Test
  public void testCalendarMetadataKeepsRowModeAndCompactFramesSeparate() throws Exception {
    VectorDeserializeRow<BinarySortableDeserializeRead> rowReader =
        new VectorDeserializeRow<>(BinarySortableDeserializeRead.ascendingNullsFirst(
            new TypeInfo[] {TypeInfoFactory.dateTypeInfo}, false));
    rowReader.init();
    Harness harness = new Harness(() -> batch(new DateColumnVector()), 1, rowReader);
    DateColumnVector dates = new DateColumnVector().setUsingProlepticCalendar(true);
    dates.vector[0] = 1;
    dates.vector[1] = 2;
    VectorizedRowBatch source = batch(dates);
    source.size = 2;
    harness.process(source);

    Output rowBytes = new Output();
    BinarySortableSerializeWrite writer = new BinarySortableSerializeWrite(1);
    writer.set(rowBytes);
    writer.writeDate(Date.ofEpochDay(9));
    BytesWritable key = new BytesWritable();
    key.setDirect(rowBytes.getData(), 0, rowBytes.getLength());
    harness.accumulator.process(key, EMPTY, false);
    assertEquals(Arrays.asList(2), harness.sizes);
    assertFalse(((DateColumnVector) harness.initial.cols[0]).isUsingProlepticCalendar());
    assertEquals(9, ((DateColumnVector) harness.initial.cols[0]).vector[0]);

    dates.setUsingProlepticCalendar(false);
    harness.process(source);
    assertEquals(Arrays.asList(2, 1), harness.sizes);
    harness.accumulator.flushPendingBatch();
    assertEquals(Arrays.asList(2, 1, 2), harness.sizes);
  }

  @Test
  public void testFullAndOneRowFramesDispatchStandalone() throws Exception {
    Harness harness = new Harness(() -> batch(4, new LongColumnVector(4)), 1, null);
    harness.process(longs(3, 10));
    harness.process(longs(3, 20));
    assertEquals(Arrays.asList(3), harness.sizes);
    harness.process(longs(4, 30));
    assertEquals(Arrays.asList(3, 3, 4), harness.sizes);
    harness.process(longs(1, 40));
    assertEquals(Arrays.asList(3, 3, 4, 1), harness.sizes);
    harness.accumulator.flushPendingBatch();
    assertEquals(Arrays.asList(3, 3, 4, 1), harness.sizes);
  }

  @Test
  public void testNestedRepeatingFramePreservesChildRepresentation() throws Exception {
    Harness harness = new Harness(
        () -> batch(new ListColumnVector(1024, new LongColumnVector())), 1, null);
    LongColumnVector child = new LongColumnVector();
    ListColumnVector list = new ListColumnVector(1024, child);
    VectorizedRowBatch source = batch(list);
    source.size = 2;
    harness.process(source);
    child.isRepeating = true;
    child.vector[0] = 42;
    list.lengths[0] = 2;
    list.childCount = 2;
    List<Boolean> childRepeating = new ArrayList<>();
    doAnswer(invocation -> {
      VectorizedRowBatch dispatched = invocation.getArgument(0);
      harness.sizes.add(dispatched.size);
      childRepeating.add(((ListColumnVector) dispatched.cols[0]).child.isRepeating);
      return null;
    }).when(harness.reducer).process(any(), anyInt());
    harness.process(source);
    assertEquals(Arrays.asList(2, 2), harness.sizes);
    assertEquals(Arrays.asList(false, true), childRepeating);
  }

  @Test
  public void testSerializedByteLimitFlushesBeforeAppendAndAfterOversizedRecord() throws Exception {
    Harness harness = new Harness(() -> batch(new LongColumnVector()), 1, null);
    VectorizedRowBatch source = longs(2, 1);
    harness.process(source);
    BytesWritable largeKey = new BytesWritable();
    largeKey.setDirect(new byte[VectorizedRowBatch.DEFAULT_BYTES], 0,
        VectorizedRowBatch.DEFAULT_BYTES);
    harness.accumulator.process(largeKey, serialize(source), true);
    assertEquals(Arrays.asList(2, 2), harness.sizes);
    assertEquals(0, harness.initial.size);
  }

  @Test
  public void testRetainedBatchesAreNotResetOrReused() throws Exception {
    Harness harness = new Harness(() -> batch(new LongColumnVector()), 1, null);
    when(harness.reducer.batchNeedsClone()).thenReturn(true);
    List<VectorizedRowBatch> retained = new ArrayList<>();
    doAnswer(invocation -> {
      retained.add(invocation.getArgument(0));
      return null;
    }).when(harness.reducer).process(any(), anyInt());
    harness.process(longs(2, 10));
    harness.accumulator.flushPendingBatch();
    harness.process(longs(2, 20));
    harness.accumulator.flushPendingBatch();
    assertEquals(2, retained.size());
    assertSame(harness.initial, retained.get(0));
    assertNotSame(retained.get(0), retained.get(1));
    assertEquals(2, retained.get(0).size);
    assertEquals(10, ((LongColumnVector) retained.get(0).cols[0]).vector[0]);
    assertEquals(20, ((LongColumnVector) retained.get(1).cols[0]).vector[0]);
  }

  @Test
  public void testRetainedBatchesReceiveMetadataThroughReplacementVectorPaths() throws Exception {
    Harness harness = new Harness(
        () -> batch(new ListColumnVector(1024, new TimestampColumnVector())), 1, null);
    when(harness.reducer.batchNeedsClone()).thenReturn(true);
    List<VectorizedRowBatch> retained = new ArrayList<>();
    doAnswer(invocation -> {
      retained.add(invocation.getArgument(0));
      return null;
    }).when(harness.reducer).process(any(), anyInt());
    for (int frame = 0; frame < 3; frame++) {
      TimestampColumnVector child = new TimestampColumnVector();
      child.setIsUTC(frame != 1);
      child.setUsingProlepticCalendar(frame == 1);
      VectorizedRowBatch source = batch(new ListColumnVector(1024, child));
      source.size = 2;
      harness.process(source);
      harness.accumulator.flushPendingBatch();
    }
    assertEquals(3, retained.size());
    for (int frame = 0; frame < retained.size(); frame++) {
      VectorizedRowBatch actual = retained.get(frame);
      assertEquals(2, actual.size);
      TimestampColumnVector child =
          (TimestampColumnVector) ((ListColumnVector) actual.cols[0]).child;
      assertEquals(frame != 1, child.isUTC());
      assertEquals(frame == 1, child.usingProlepticCalendar());
      if (frame > 0) {
        assertNotSame(retained.get(frame - 1), actual);
      }
    }
  }

  @Test
  public void testReuseRestoresProjectionSelectionAndScratchMetadata() throws Exception {
    Harness harness = new Harness(
        () -> batch(new LongColumnVector(), new DateColumnVector()), 1, null);
    int[] projection = harness.initial.projectedColumns;
    int[] selection = harness.initial.selected;
    doAnswer(invocation -> {
      VectorizedRowBatch dispatched = invocation.getArgument(0);
      dispatched.projectedColumns = new int[] {1};
      dispatched.projectionSize = 0;
      dispatched.selected = new int[] {17};
      dispatched.selectedInUse = true;
      dispatched.endOfFile = true;
      dispatched.size = 0;
      ((DateColumnVector) dispatched.cols[1]).setUsingProlepticCalendar(true);
      return null;
    }).when(harness.reducer).process(any(), anyInt());
    harness.process(longs(2, 10));
    harness.accumulator.flushPendingBatch();
    assertSame(projection, harness.initial.projectedColumns);
    assertSame(selection, harness.initial.selected);
    assertEquals(1, harness.initial.projectionSize);
    assertEquals(0, harness.initial.projectedColumns[0]);
    assertFalse(harness.initial.selectedInUse);
    assertFalse(harness.initial.endOfFile);
    assertFalse(((DateColumnVector) harness.initial.cols[1]).isUsingProlepticCalendar());
    harness.process(longs(2, 20));
    assertEquals(2, harness.initial.size);
    assertEquals(20, ((LongColumnVector) harness.initial.cols[0]).vector[0]);
  }

  @Test
  public void testEmptyAndNegativeFramesDoNotPublishPendingRowsAndAbortDiscards() throws Exception {
    Harness harness = new Harness(() -> batch(new LongColumnVector()), 1, null);
    harness.process(longs(2, 10));
    harness.process(longs(0, 0));
    assertTrue(harness.sizes.isEmpty());
    assertEquals(2, harness.initial.size);
    // Mutate only the header: negative frames must be rejected before pending-batch mutation.
    BytesWritable negative = serialize(longs(0, 0));
    org.apache.tez.util.FastByteComparisons.theUnsafe.putInt(negative.getBytesRaw(),
        org.apache.tez.util.FastByteComparisons.BYTE_ARRAY_BASE_OFFSET + negative.getOffset(), -1);
    assertThrows(IOException.class, () -> harness.accumulator.process(EMPTY, negative, true));
    assertEquals(2, harness.initial.size);
    harness.accumulator.discardPendingBatch();
    harness.accumulator.flushPendingBatch();
    assertTrue(harness.sizes.isEmpty());
  }

  private static VectorizedRowBatch longs(int rows, long first) {
    LongColumnVector values = new LongColumnVector();
    VectorizedRowBatch result = batch(values);
    result.size = rows;
    for (int row = 0; row < rows; row++) {
      values.vector[row] = first + row;
    }
    return result;
  }

  private static VectorizedRowBatch batch(ColumnVector... columns) {
    return batch(VectorizedRowBatch.DEFAULT_SIZE, columns);
  }

  private static VectorizedRowBatch batch(int capacity, ColumnVector... columns) {
    VectorizedRowBatch result = new VectorizedRowBatch(columns.length, capacity);
    System.arraycopy(columns, 0, result.cols, 0, columns.length);
    result.reset();
    return result;
  }

  private static BytesWritable serialize(VectorizedRowBatch source) {
    int[] columns = new int[source.numCols];
    for (int column = 0; column < columns.length; column++) {
      columns[column] = column;
    }
    byte[] bytes = new byte[64 * 1024];
    int length = new VectorShuffleBatchSerializer().serialize(source, columns, bytes, 0);
    BytesWritable result = new BytesWritable();
    result.setDirect(bytes, 0, length);
    return result;
  }

  private static final class Harness {
    private final Operator<?> reducer = mock(Operator.class);
    private final VectorizedRowBatch initial;
    private final VectorShuffleBatchAccumulator accumulator;
    private final List<Integer> sizes = new ArrayList<>();

    private Harness(Supplier<VectorizedRowBatch> factory, int inputColumns,
        VectorDeserializeRow<BinarySortableDeserializeRead> rowReader) throws Exception {
      initial = factory.get();
      VectorizedRowBatchCtx context = mock(VectorizedRowBatchCtx.class);
      when(context.createVectorizedRowBatch()).thenAnswer(invocation -> factory.get());
      doAnswer(invocation -> {
        VectorizedRowBatch dispatched = invocation.getArgument(0);
        sizes.add(dispatched.size);
        return null;
      }).when(reducer).process(any(), anyInt());
      accumulator = new VectorShuffleBatchAccumulator(context, reducer, initial, inputColumns,
          (byte) 0, rowReader, null);
    }

    private void process(VectorizedRowBatch source) throws Exception {
      accumulator.process(EMPTY, serialize(source), true);
    }
  }
}
