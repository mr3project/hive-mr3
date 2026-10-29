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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.junit.Test;

public class TestVectorShuffleBatchLowerBound {
  @Test
  public void testFixedWidthsEmptyStringsAndDuplicateMappings() {
    BytesColumnVector strings = new BytesColumnVector();
    for (int row = 0; row < 6; row++) {
      strings.setRef(row, new byte[0], 0, 0);
    }
    VectorizedRowBatch source = batch(6, new LongColumnVector(), new DoubleColumnVector(),
        new DateColumnVector(), new Decimal64ColumnVector(18, 2), new TimestampColumnVector(),
        new IntervalDayTimeColumnVector(), strings);
    int[] mapping = {6, 0, 2, 4, 1, 3, 5, 0};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    byte[] buffer = new byte[1024];
    serializer.initialize(source.cols, mapping);
    serializer.setupCommonPrefix(source, mapping, buffer, 11, 2);
    assertEquals(19 + 68L * Integer.MAX_VALUE,
        serializer.getPreparedPartitionLowerBound(Integer.MAX_VALUE));
    int[] rows = {5, 1, 3, 0, 4, 2};
    for (int rowOffset : new int[] {0, 3}) {
      int expected = directLength(source, mapping, rows, rowOffset, 3);
      assertEquals(expected, serializer.getPreparedPartitionLowerBound(3));
      assertEquals(expected, serializer.serializePartition(source, rows, rowOffset, 3,
          buffer, 11, expected));
    }
  }

  @Test
  public void testEmptyAndInactiveNestedChildrenStillContributeFlags() {
    ListColumnVector list = new ListColumnVector(8, new DateColumnVector());
    MapColumnVector map = new MapColumnVector(8, new BytesColumnVector(), new LongColumnVector());
    StructColumnVector struct = new StructColumnVector(8,
        new DoubleColumnVector(), new TimestampColumnVector());
    UnionColumnVector union = new UnionColumnVector(8, new VoidColumnVector(),
        new ListColumnVector(8, new LongColumnVector()));
    map.noNulls = false;
    struct.noNulls = false;
    Arrays.fill(map.isNull, true);
    Arrays.fill(struct.isNull, true);
    VectorizedRowBatch source = batch(3, list, map, struct, union);
    int[] mapping = {0, 1, 2, 3};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    serializer.initialize(source.cols, mapping);
    serializer.setupCommonPrefix(source, mapping, new byte[1024], 0, 2);
    // Six prefix bytes, five partition-header bytes, twelve nodes, eight bytes per row.
    assertEquals(47, serializer.getPreparedPartitionLowerBound(3));
    assertEquals(49, directLength(source, mapping, new int[] {0, 1, 2}, 0, 3));
  }

  @Test
  public void testBatchChangesRefreshRepeatingNullabilityAndDecimalRepresentation() {
    BytesColumnVector strings = new BytesColumnVector();
    strings.isRepeating = true;
    strings.setRef(0, new byte[200], 0, 200);
    LongColumnVector longs = new LongColumnVector();
    longs.noNulls = false;
    Arrays.fill(longs.isNull, true);
    VectorizedRowBatch source = batch(2, strings, longs, new Decimal64ColumnVector(18, 2));
    int[] mapping = {0, 1, 2};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    serializer.initialize(source.cols, mapping);
    serializer.setupCommonPrefix(source, mapping, new byte[1024], 0, 2);
    assertEquals(237, serializer.getPreparedPartitionLowerBound(2));
    assertTrue(serializer.getPreparedPartitionLowerBound(2)
        <= directLength(source, mapping, new int[] {0, 1}, 0, 2));

    strings.isRepeating = false;
    for (int row = 0; row < 2; row++) {
      strings.setRef(row, new byte[0], 0, 0);
    }
    longs.noNulls = true;
    DecimalColumnVector decimals = new DecimalColumnVector(18, 2);
    decimals.vector[0].set(HiveDecimal.create("1.25"));
    decimals.vector[1].set(HiveDecimal.create("2.50"));
    source.cols[2] = decimals;
    // Reuse the initialized schema: decimal64 can be replaced by arbitrary decimal vectors.
    serializer.setupCommonPrefix(source, mapping, new byte[1024], 0, 2);
    assertEquals(37, serializer.getPreparedPartitionLowerBound(2));
    assertTrue(serializer.getPreparedPartitionLowerBound(2)
        <= directLength(source, mapping, new int[] {0, 1}, 0, 2));
  }

  @Test
  public void testRejectedFirstPartitionPreservesOwnedPrefixAfterWriterReuse() {
    BytesColumnVector strings = new BytesColumnVector();
    strings.isRepeating = true;
    strings.setRef(0, new byte[20], 0, 20);
    VectorizedRowBatch source = batch(6, strings, new LongColumnVector());
    int[] mapping = {0, 1};
    int[] rows = {0, 1, 2, 3, 4, 5};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    serializer.initialize(source.cols, mapping);
    byte[] buffer = new byte[256];
    serializer.setupCommonPrefix(source, mapping, buffer, 7, 2);
    byte[] beforeRejection = buffer.clone();
    assertEquals(72, serializer.getPreparedPartitionLowerBound(4));
    assertEquals(0, serializer.serializePartition(source, rows, 0, 4, buffer, 7, 71));
    assertArrayEquals(beforeRejection, buffer);
    Arrays.fill(buffer, (byte) 0x5a); // Row fallback overwrites the first writer region.
    assertEquals(56, serializer.serializePartition(source, rows, 4, 2, buffer, 7, 56));
    VectorShuffleBatchSerializer direct = new VectorShuffleBatchSerializer();
    direct.initialize(source.cols, mapping);
    byte[] expected = new byte[256];
    Arrays.fill(expected, (byte) 0x5a);
    assertEquals(56, direct.serialize(source, mapping, rows, 4, 2, expected, 7));
    assertArrayEquals(expected, buffer);
  }

  @Test
  public void testPassingBoundStillAllowsPayloadOverflowFallback() {
    BytesColumnVector strings = new BytesColumnVector();
    strings.setRef(0, new byte[256], 0, 256);
    strings.setRef(1, new byte[256], 0, 256);
    VectorizedRowBatch source = batch(2, strings);
    int[] mapping = {0};
    int[] rows = {0, 1};
    VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
    serializer.initialize(source.cols, mapping);
    byte[] limited = new byte[32];
    serializer.setupCommonPrefix(source, mapping, limited, 0, 2);
    assertEquals(19, serializer.getPreparedPartitionLowerBound(2));
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> serializer.serializePartition(source, rows, 0, 2, limited, 0, limited.length));
    int expected = directLength(source, mapping, rows, 0, 2);
    assertEquals(expected, serializer.serializePartition(source, rows, 0, 2,
        new byte[expected], 0, expected));
  }

  private static int directLength(VectorizedRowBatch source, int[] mapping, int[] rows,
      int rowOffset, int rowCount) {
    VectorShuffleBatchSerializer direct = new VectorShuffleBatchSerializer();
    direct.initialize(source.cols, mapping);
    return direct.serialize(source, mapping, rows, rowOffset, rowCount, new byte[4096], 0);
  }

  private static VectorizedRowBatch batch(int rows, ColumnVector... columns) {
    VectorizedRowBatch source = new VectorizedRowBatch(columns.length);
    System.arraycopy(columns, 0, source.cols, 0, columns.length);
    source.size = rows;
    return source;
  }
}
