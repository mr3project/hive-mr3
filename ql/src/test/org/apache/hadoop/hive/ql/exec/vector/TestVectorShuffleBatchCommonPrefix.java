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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer.CommonPrefix;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchCommonPrefix {
  private static final int BUFFER_SIZE = 4096;
  private static final int FRAME_OFFSET = 11;

  @Test
  public void testPreparedPartitionsMatchDirectFramesWithComplexNullsAndMetadata()
      throws Exception {
    LongColumnVector ordinary = new LongColumnVector();
    TimestampColumnVector timestamps = new TimestampColumnVector();
    timestamps.setIsUTC(true);
    timestamps.setUsingProlepticCalendar(true);
    for (int row = 0; row < 6; row++) {
      ordinary.vector[row] = 100 + row;
      timestamps.time[row] = 1000 + row;
      timestamps.nanos[row] = row;
    }
    BytesColumnVector repeatedNull = new BytesColumnVector();
    repeatedNull.isRepeating = true;
    repeatedNull.noNulls = false;
    repeatedNull.isNull[0] = true;
    LongColumnVector elements = new LongColumnVector();
    elements.vector[0] = 11;
    elements.vector[1] = 12;
    ListColumnVector repeatedList = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, elements);
    repeatedList.isRepeating = true;
    repeatedList.offsets[0] = 0;
    repeatedList.lengths[0] = 2;
    repeatedList.childCount = 2;
    DateColumnVector date = new DateColumnVector().setUsingProlepticCalendar(true);
    date.isRepeating = true;
    date.vector[0] = -200000;
    VectorizedRowBatch source = batch(6, ordinary, repeatedNull, repeatedList, date, timestamps);
    int[] mapping = {2, 0, 3, 1, 4};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    byte[] preparationBuffer = new byte[BUFFER_SIZE];
    Arrays.fill(preparationBuffer, (byte) 0x5a);
    prepareCommonPrefix(serializer, source, mapping, preparationBuffer, 7, 2);
    // The initial writer may reuse its buffer before the next partition is emitted.
    int[] indices = {99, 5, 1, 3, 99, 4, 0, 99};
    byte[] commonBytes = null;
    for (int[] partition : new int[][] {{1, 3}, {5, 2}}) {
      BytesWritable frame;
      if (commonBytes == null) {
        frame = assertFirstPreparedFrame(serializer, source, mapping, indices,
            partition[0], partition[1], preparationBuffer, 7);
      } else {
        Arrays.fill(preparationBuffer, (byte) 0xff);
        frame = assertPreparedFrame(serializer, source, mapping, indices,
            partition[0], partition[1]);
      }
      VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
      header.read(frame, mapping.length, 1);
      assertEquals(mapping.length, ByteBuffer.wrap(frame.getBytesRaw())
          .order(ByteOrder.nativeOrder()).getInt(frame.getOffset()));
      assertEquals(header.getRepeatingEnd() + Integer.BYTES + 1, header.getOrdinaryStart());
      byte[] actualCommon = Arrays.copyOfRange(frame.getBytesRaw(), frame.getOffset(),
          header.getRepeatingEnd());
      if (commonBytes == null) {
        commonBytes = actualCommon;
      } else {
        assertArrayEquals(commonBytes, actualCommon);
      }
      assertFalse(header.hasNestedRepeating());
      for (boolean accumulating : new boolean[] {false, true}) {
        VectorizedRowBatch decoded = batch(0,
            new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()),
            new LongColumnVector(), new DateColumnVector(), new BytesColumnVector(),
            new TimestampColumnVector());
        if (accumulating) {
          new VectorShuffleBatchAccumulatingDeserializer(decoded, mapping.length)
              .deserialize(frame, decoded);
        } else {
          new VectorShuffleBatchDeserializer().deserialize(frame, decoded);
        }
        assertEquals(partition[1], decoded.size);
        ListColumnVector actualList = (ListColumnVector) decoded.cols[0];
        assertTrue(actualList.isRepeating);
        assertEquals(2, actualList.lengths[0]);
        assertEquals(11, ((LongColumnVector) actualList.child).vector[0]);
        assertEquals(12, ((LongColumnVector) actualList.child).vector[1]);
        assertTrue(decoded.cols[3].isRepeating);
        assertTrue(decoded.cols[3].isNull[0]);
        assertEquals(-200000, ((DateColumnVector) decoded.cols[2]).vector[0]);
        assertTrue(((DateColumnVector) decoded.cols[2]).isUsingProlepticCalendar());
        TimestampColumnVector actualTime = (TimestampColumnVector) decoded.cols[4];
        assertTrue(actualTime.isUTC());
        assertTrue(actualTime.usingProlepticCalendar());
        for (int row = 0; row < decoded.size; row++) {
          int original = indices[partition[0] + row];
          assertEquals(100 + original, ((LongColumnVector) decoded.cols[1]).vector[row]);
          assertEquals(1000 + original, actualTime.time[row]);
          assertEquals(original, actualTime.nanos[row]);
        }
      }
    }
  }

  @Test
  public void testSingleCompactPartitionSkipsOwnedPrefixCopy() throws Exception {
    LongColumnVector ordinary = new LongColumnVector();
    for (int row = 0; row < 4; row++) {
      ordinary.vector[row] = 100 + row;
    }
    BytesColumnVector repeated = new BytesColumnVector();
    repeated.isRepeating = true;
    byte[] repeatedBytes = new byte[256];
    Arrays.fill(repeatedBytes, (byte) 37);
    repeated.setRef(0, repeatedBytes, 0, repeatedBytes.length);

    VectorizedRowBatch source = batch(4, repeated, ordinary);
    int[] mapping = {0, 1};
    int[] indices = {0, 2, 1, 3};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    Field bytes = CommonPrefix.class.getDeclaredField("bytes");
    bytes.setAccessible(true);
    Field prepared = VectorShuffleBatchSerializer.class.getDeclaredField("preparedPrefix");
    prepared.setAccessible(true);

    byte[] singleBuffer = new byte[BUFFER_SIZE];
    Arrays.fill(singleBuffer, (byte) 0x5a);
    prepareCommonPrefix(serializer, source, mapping, singleBuffer, FRAME_OFFSET, 1);
    CommonPrefix prefix = (CommonPrefix) prepared.get(serializer);
    assertEquals(0, ((byte[]) bytes.get(prefix)).length);
    assertFirstPreparedFrame(serializer, source, mapping, indices,
        0, 2, singleBuffer, FRAME_OFFSET);

    byte[] multiBuffer = new byte[BUFFER_SIZE];
    Arrays.fill(multiBuffer, (byte) 0x5a);
    prepareCommonPrefix(serializer, source, mapping, multiBuffer, FRAME_OFFSET, 2);
    assertSame(prefix, prepared.get(serializer));
    byte[] cachedBytes = (byte[]) bytes.get(prefix);
    assertTrue(cachedBytes.length > 0);
    assertFirstPreparedFrame(serializer, source, mapping, indices,
        0, 2, multiBuffer, FRAME_OFFSET);
    assertPreparedFrame(serializer, source, mapping, indices, 2, 2);

    // A single-partition batch must reuse storage without copying stale prefix bytes.
    byte[] nextSingleBuffer = new byte[BUFFER_SIZE];
    Arrays.fill(nextSingleBuffer, (byte) 0x5a);
    prepareCommonPrefix(serializer, source, mapping, nextSingleBuffer, FRAME_OFFSET, 1);
    assertSame(prefix, prepared.get(serializer));
    assertSame(cachedBytes, bytes.get(prefix));
    assertFirstPreparedFrame(serializer, source, mapping, indices,
        2, 2, nextSingleBuffer, FRAME_OFFSET);
  }

  @Test
  public void testPartitionNestedFlagIncludesBothPayloadSections() throws Exception {
    for (boolean repeatingSectionNested : new boolean[] {false, true}) {
      LongColumnVector repeatedChild = new LongColumnVector();
      repeatedChild.isRepeating = repeatingSectionNested;
      repeatedChild.vector[0] = 17;
      repeatedChild.vector[1] = 18;
      ListColumnVector repeated = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
          repeatedChild);
      repeated.isRepeating = true;
      repeated.lengths[0] = 2;
      repeated.childCount = 2;
      LongColumnVector ordinaryChild = new LongColumnVector();
      ordinaryChild.isRepeating = true;
      ordinaryChild.vector[0] = 23;
      ListColumnVector ordinary = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
          ordinaryChild);
      ordinary.offsets[1] = 0;
      ordinary.lengths[1] = 1;
      ordinary.offsets[2] = 1;
      ordinary.offsets[3] = 1;
      ordinary.lengths[3] = 2;
      ordinary.childCount = 3;
      VectorizedRowBatch source = batch(4, repeated, ordinary);
      int[] mapping = {0, 1};
      VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
      prepareCommonPrefix(serializer, source, mapping,
          new byte[BUFFER_SIZE], 0, 2);
      int[] indices = {0, 1, 2, 3};
      for (int partition = 0; partition < 2; partition++) {
        BytesWritable frame = assertPreparedFrame(serializer, source, mapping, indices,
            partition * 2, 2);
        VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
        header.read(frame, 2, 0);
        assertEquals(repeatingSectionNested || partition == 1, header.hasNestedRepeating());
        VectorizedRowBatch decoded = batch(0,
            new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()),
            new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()));
        new VectorShuffleBatchDeserializer(decoded, 2).deserialize(frame, decoded);
        ListColumnVector actual = (ListColumnVector) decoded.cols[1];
        assertEquals(partition + 1, actual.lengths[1]);
        assertEquals(23, ((LongColumnVector) actual.child).vector[0]);
      }
    }
  }

  @Test
  public void testNoRepeatingPrefixSurvivesCapacityFailureAndRetry() throws Exception {
    LongColumnVector numbers = new LongColumnVector();
    BytesColumnVector strings = new BytesColumnVector();
    strings.initBuffer();
    for (int row = 0; row < 5; row++) {
      numbers.vector[row] = row;
      strings.setVal(row, ("value-" + row).getBytes(StandardCharsets.UTF_8));
    }
    strings.noNulls = false;
    strings.isNull[2] = true;
    VectorizedRowBatch source = batch(5, numbers, strings);
    int[] mapping = {0, 1};
    int[] indices = {0, 1, 2, 3, 4};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    serializer.initialize(source.cols, mapping);
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> prepareCommonPrefix(serializer, source, mapping, new byte[3], 0, 2));
    byte[] limitedBuffer = new byte[16];
    prepareCommonPrefix(serializer, source, mapping, limitedBuffer, 0, 2);
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> serializer.serializePartition(source, indices, 0, 3,
            limitedBuffer, 0));
    prepareCommonPrefix(serializer, source, mapping,
        new byte[BUFFER_SIZE], 0, 2);
    BytesWritable expected = assertPreparedFrame(serializer, source, mapping, indices, 0, 3);
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(expected, 2, 0);
    assertFalse(header.hasRepeatingColumns());
    // Generic serialization uses the serializer's classification scratch, not the prefix's copy.
    LongColumnVector otherNumbers = new LongColumnVector();
    otherNumbers.isRepeating = true;
    BytesColumnVector otherStrings = new BytesColumnVector();
    otherStrings.initBuffer();
    otherStrings.isRepeating = true;
    otherStrings.setVal(0, "other".getBytes(StandardCharsets.UTF_8));
    serializer.serialize(batch(2, otherNumbers, otherStrings), mapping,
        new byte[BUFFER_SIZE], 0);
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> serializer.serializePartition(
        source, indices, 0, 3, new byte[expected.getLength() - 1], 0));
    BytesWritable retried = assertPreparedFrame(serializer, source, mapping, indices, 0, 3);
    assertArrayEquals(expected.copyBytes(), retried.copyBytes());
    BytesWritable next = assertPreparedFrame(serializer, source, mapping, indices, 3, 2);
    VectorizedRowBatch decoded = batch(0, new LongColumnVector(), new BytesColumnVector());
    new VectorShuffleBatchAccumulatingDeserializer(decoded, 2).deserialize(next, decoded);
    assertEquals(2, decoded.size);
    assertEquals(3, ((LongColumnVector) decoded.cols[0]).vector[0]);
    assertEquals(4, ((LongColumnVector) decoded.cols[0]).vector[1]);
    assertFalse(decoded.cols[1].isNull[0]);
  }

  @Test
  public void testPreparationRetriesAfterRepeatingPayloadExceedsWriterBuffer() {
    BytesColumnVector repeated = new BytesColumnVector();
    repeated.initBuffer();
    repeated.isRepeating = true;
    repeated.setVal(0, new byte[256]);
    VectorizedRowBatch source = batch(2, repeated, new LongColumnVector());
    int[] mapping = {0, 1};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> prepareCommonPrefix(serializer, source, mapping, new byte[32], 0, 2));
    prepareCommonPrefix(serializer, source, mapping,
        new byte[BUFFER_SIZE], 0, 2);
    assertPreparedFrame(serializer, source, mapping, new int[] {0, 1}, 0, 2);
  }

  @Test
  public void testPreparedWriterSupportsValidBufferOffsets() {
    VectorizedRowBatch source = batch(2, new LongColumnVector());
    int[] mapping = {0};
    int[] indices = {0, 1};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    for (int offset : new int[] {3, 11, BUFFER_SIZE - 32}) {
      byte[] buffer = new byte[BUFFER_SIZE];
      Arrays.fill(buffer, (byte) 0x5a);
      prepareCommonPrefix(serializer, source, mapping, buffer, offset, 2);
      assertFirstPreparedFrame(serializer, source, mapping, indices,
          0, 2, buffer, offset);
    }
  }

  @Test
  public void testPreparationRefreshesReusedBatchAndPreservesPriorFrames() throws Exception {
    DateColumnVector dates = new DateColumnVector().setUsingProlepticCalendar(true);
    dates.isRepeating = true;
    dates.vector[0] = -200000;
    dates.vector[1] = -199999;
    VectorizedRowBatch source = batch(2, dates);
    int[] mapping = {0};
    int[] indices = {0, 1};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    prepareCommonPrefix(serializer, source, mapping,
        new byte[BUFFER_SIZE], 0, 2);
    BytesWritable first = assertPreparedFrame(serializer, source, mapping, indices, 0, 2);
    byte[] firstBytes = first.copyBytes();
    dates.isRepeating = false;
    dates.setUsingProlepticCalendar(false);
    dates.vector[0] = 10;
    dates.vector[1] = 11;
    prepareCommonPrefix(serializer, source, mapping,
        new byte[BUFFER_SIZE], 0, 2);
    BytesWritable second = assertPreparedFrame(serializer, source, mapping,
        indices, 0, 2);
    assertArrayEquals(firstBytes, first.copyBytes());
    VectorizedRowBatch decoded = batch(0, new DateColumnVector());
    new VectorShuffleBatchDeserializer().deserialize(first, decoded);
    assertTrue(decoded.cols[0].isRepeating);
    assertTrue(((DateColumnVector) decoded.cols[0]).isUsingProlepticCalendar());
    assertEquals(-200000, ((DateColumnVector) decoded.cols[0]).vector[0]);
    new VectorShuffleBatchDeserializer().deserialize(second, decoded);
    assertFalse(decoded.cols[0].isRepeating);
    assertFalse(((DateColumnVector) decoded.cols[0]).isUsingProlepticCalendar());
    assertEquals(10, ((DateColumnVector) decoded.cols[0]).vector[0]);
    assertEquals(11, ((DateColumnVector) decoded.cols[0]).vector[1]);
  }

  @Test
  public void testHeaderRejectsTruncatedPartitionHeader() throws Exception {
    for (boolean repeating : new boolean[] {false, true}) {
      LongColumnVector numbers = new LongColumnVector();
      numbers.isRepeating = repeating;
      VectorizedRowBatch source = batch(2, numbers);
      VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
      int[] mapping = {0};
      prepareCommonPrefix(serializer, source, mapping,
          new byte[BUFFER_SIZE], 0, 2);
      BytesWritable frame = assertPreparedFrame(serializer, source, mapping,
          new int[] {0, 1}, 0, 2);
      VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
      header.read(frame, 1, 0);
      int commonLength = header.getRepeatingEnd() - frame.getOffset();
      for (int bytes = 0; bytes < Integer.BYTES + 1; bytes++) {
        BytesWritable truncated = new BytesWritable(frame.getBytesRaw(), frame.getOffset(),
            commonLength + bytes);
        assertThrows(IOException.class,
            () -> new VectorShuffleBatchHeader().read(truncated, 1, 0));
      }
    }
  }

  @Test
  public void testPreparedWriterRejectsInsufficientBufferCapacity() {
    VectorizedRowBatch source = batch(2, new LongColumnVector());
    int[] mapping = {0};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    byte[] buffer = new byte[BUFFER_SIZE];
    for (int offset : new int[] {BUFFER_SIZE - 1, BUFFER_SIZE}) {
      assertThrows(ArrayIndexOutOfBoundsException.class,
          () -> prepareCommonPrefix(serializer, source, mapping, buffer, offset, 2));
    }
    prepareCommonPrefix(serializer, source, mapping, buffer, 0, 2);
    for (int offset : new int[] {BUFFER_SIZE - 1, BUFFER_SIZE}) {
      assertThrows(ArrayIndexOutOfBoundsException.class,
          () -> serializer.serializePartition(source,
              new int[] {0, 1}, 0, 2, buffer, offset));
    }
  }

  private static void prepareCommonPrefix(VectorShuffleBatchSerializer serializer,
      VectorizedRowBatch source, int[] mapping, byte[] buffer, int position,
      int compactCandidateCount) {
    serializer.initialize(source.cols, mapping);
    serializer.setupCommonPrefix(source, mapping, buffer, position, compactCandidateCount);
  }

  private static BytesWritable assertFirstPreparedFrame(VectorShuffleBatchSerializer serializer,
      VectorizedRowBatch source, int[] mapping, int[] indices,
      int rowOffset, int rowCount, byte[] buffer, int offset) {
    byte[] expected = Arrays.copyOf(buffer, buffer.length);
    VectorShuffleBatchSerializer direct = new VectorShuffleBatchSerializer();
    direct.initialize(source.cols, mapping);
    int directLength = direct.serialize(source, mapping, indices, rowOffset, rowCount,
        expected, offset);
    int length = serializer.serializePartition(source, indices, rowOffset, rowCount,
        buffer, offset);
    assertEquals(directLength, length);
    assertArrayEquals(expected, buffer);
    assertEquals(0x5a, buffer[offset - 1] & 0xff);
    assertEquals(0x5a, buffer[offset + length] & 0xff);
    return new BytesWritable(buffer, offset, length);
  }

  private static BytesWritable assertPreparedFrame(VectorShuffleBatchSerializer serializer,
      VectorizedRowBatch source, int[] mapping, int[] indices,
      int rowOffset, int rowCount) {
    byte[] actual = new byte[BUFFER_SIZE];
    byte[] expected = new byte[BUFFER_SIZE];
    Arrays.fill(actual, (byte) 0x5a);
    Arrays.fill(expected, (byte) 0x5a);
    int length = serializer.serializePartition(source, indices, rowOffset, rowCount,
        actual, FRAME_OFFSET);
    VectorShuffleBatchSerializer direct = new VectorShuffleBatchSerializer();
    direct.initialize(source.cols, mapping);
    int directLength = direct.serialize(source, mapping, indices, rowOffset, rowCount,
        expected, FRAME_OFFSET);
    assertEquals(directLength, length);
    assertArrayEquals(expected, actual);
    assertEquals(0x5a, actual[FRAME_OFFSET - 1]);
    assertEquals(0x5a, actual[FRAME_OFFSET + length]);
    return new BytesWritable(actual, FRAME_OFFSET, length);
  }

  private static VectorizedRowBatch batch(int rows, ColumnVector... columns) {
    VectorizedRowBatch batch = new VectorizedRowBatch(columns.length);
    System.arraycopy(columns, 0, batch.cols, 0, columns.length);
    batch.size = rows;
    return batch;
  }
}
