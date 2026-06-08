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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchSerde {
  private static final long BIGINT_PROPERTY_TEST_SEED = 0x5EEDB16L;
  private static final long STRING_PROPERTY_TEST_SEED = 0x5EED517L;
  private static final long DECIMAL_PROPERTY_TEST_SEED = 0x5EEDDEC1L;
  private static final long ARRAY_BIGINT_PROPERTY_TEST_SEED = 0xA22A7B16L;
  private static final long ARRAY_STRING_PROPERTY_TEST_SEED = 0xA22A7517L;
  private static final long ARRAY_DECIMAL_PROPERTY_TEST_SEED = 0xA22ADEC1L;
  private static final long NESTED_ARRAY_PROPERTY_TEST_SEED = 0xA22AA22AL;
  private static final int BIGINT_PROPERTY_TEST_ITERATIONS = 10000;
  private static final int PROPERTY_TEST_ITERATIONS = 200;
  private static final int ARRAY_PROPERTY_TEST_ITERATIONS = 1000;
  private static final int NESTED_ARRAY_PROPERTY_TEST_ITERATIONS = 500;
  private static final int MAX_ARRAY_LENGTH = 16;
  private static final int DECIMAL_PRECISION = 20;
  private static final int DECIMAL_SCALE = 4;
  private static final int MAX_LOGGED_PROPERTY_TEST_CASES = 100;
  private static final String COLUMN_LOG_PREFIX = "TestVectorShuffleBatchSerde-";
  private static final long[] INTERESTING_BIGINTS = {
      Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE, -11, -10, -1, 0, 1, 10, 11,
      Integer.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE
  };
  private static final byte[][] INTERESTING_STRINGS = {
      bytes(""), bytes("a"), bytes("alpha"), bytes("with spaces"), bytes("\0embedded"),
      bytes("Hive \u2603 \ud83d\udc1d")
  };
  private static final HiveDecimal[] INTERESTING_DECIMALS = {
      HiveDecimal.create("-9999999999999999.9999"), HiveDecimal.create("-1.0000"),
      HiveDecimal.create("-0.0001"), HiveDecimal.ZERO, HiveDecimal.create("0.0001"),
      HiveDecimal.create("1.0000"), HiveDecimal.create("9999999999999999.9999")
  };

  private final VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
  private final VectorShuffleBatchDeserializer deserializer = new VectorShuffleBatchDeserializer();

  @Test
  public void testBigIntColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("BIGINT", BIGINT_PROPERTY_TEST_SEED,
        BIGINT_PROPERTY_TEST_ITERATIONS, bigIntAdapter());
  }

  @Test
  public void testStringColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("STRING", STRING_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
        stringAdapter());
  }

  @Test
  public void testDecimalColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("DECIMAL", DECIMAL_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
        decimalAdapter());
  }

  @Test
  public void testArrayOfBigIntColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<BIGINT>", ARRAY_BIGINT_PROPERTY_TEST_SEED,
        ARRAY_PROPERTY_TEST_ITERATIONS, arrayAdapter(bigIntAdapter()));
  }

  @Test
  public void testArrayOfStringColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<STRING>", ARRAY_STRING_PROPERTY_TEST_SEED,
        ARRAY_PROPERTY_TEST_ITERATIONS, arrayAdapter(stringAdapter()));
  }

  @Test
  public void testArrayOfDecimalColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<DECIMAL>", ARRAY_DECIMAL_PROPERTY_TEST_SEED,
        ARRAY_PROPERTY_TEST_ITERATIONS, arrayAdapter(decimalAdapter()));
  }

  @Test
  public void testNestedArrayColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<ARRAY<BIGINT>>", NESTED_ARRAY_PROPERTY_TEST_SEED,
        NESTED_ARRAY_PROPERTY_TEST_ITERATIONS, arrayAdapter(arrayAdapter(bigIntAdapter())));
  }

  @Test
  public void testSparseArrayOfBigIntColumnSchema() throws Exception {
    LongColumnVector sourceElements = new LongColumnVector();
    ListColumnVector sourceLists =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, sourceElements);
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    source.cols[0] = sourceLists;
    source.size = 2;
    sourceLists.offsets[0] = 2;
    sourceLists.lengths[0] = 2;
    sourceLists.offsets[1] = 7;
    sourceLists.lengths[1] = 1;
    sourceLists.childCount = 8;
    sourceElements.vector[2] = 20;
    sourceElements.vector[3] = 30;
    sourceElements.vector[7] = 70;

    ListColumnVector resultLists =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector());
    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = resultLists;
    roundTrip(source, new int[] {0}, result);

    LongColumnVector resultElements = (LongColumnVector) resultLists.child;
    assertEquals(3, resultLists.childCount);
    assertEquals(0, resultLists.offsets[0]);
    assertEquals(2, resultLists.lengths[0]);
    assertEquals(2, resultLists.offsets[1]);
    assertEquals(1, resultLists.lengths[1]);
    assertEquals(20, resultElements.vector[0]);
    assertEquals(30, resultElements.vector[1]);
    assertEquals(70, resultElements.vector[2]);
  }

  @Test
  public void testMapOfStringToBigIntColumnSchema() throws Exception {
    BytesColumnVector sourceKeys = new BytesColumnVector();
    sourceKeys.initBuffer();
    LongColumnVector sourceValues = new LongColumnVector();
    MapColumnVector sourceMaps =
        new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, sourceKeys, sourceValues);
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    source.cols[0] = sourceMaps;
    source.size = 2;
    sourceMaps.offsets[0] = 1;
    sourceMaps.lengths[0] = 2;
    sourceMaps.offsets[1] = 5;
    sourceMaps.lengths[1] = 1;
    sourceMaps.childCount = 6;
    sourceKeys.setVal(1, bytes("one"));
    sourceKeys.setVal(2, bytes("two"));
    sourceKeys.setVal(5, bytes("five"));
    sourceValues.vector[1] = 1;
    sourceValues.vector[2] = 2;
    sourceValues.vector[5] = 5;

    BytesColumnVector resultKeys = new BytesColumnVector();
    LongColumnVector resultValues = new LongColumnVector();
    MapColumnVector resultMaps =
        new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, resultKeys, resultValues);
    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = resultMaps;
    roundTrip(source, new int[] {0}, result);

    assertEquals(3, resultMaps.childCount);
    assertEquals(0, resultMaps.offsets[0]);
    assertEquals(2, resultMaps.lengths[0]);
    assertEquals(2, resultMaps.offsets[1]);
    assertEquals(1, resultMaps.lengths[1]);
    assertBytes("one", resultKeys, 0);
    assertBytes("two", resultKeys, 1);
    assertBytes("five", resultKeys, 2);
    assertEquals(1, resultValues.vector[0]);
    assertEquals(2, resultValues.vector[1]);
    assertEquals(5, resultValues.vector[2]);
  }

  @Test
  public void testArrayOfMapOfStringToBigIntColumnSchema() throws Exception {
    BytesColumnVector sourceKeys = new BytesColumnVector();
    sourceKeys.initBuffer();
    LongColumnVector sourceValues = new LongColumnVector();
    MapColumnVector sourceMaps =
        new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, sourceKeys, sourceValues);
    ListColumnVector sourceLists =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, sourceMaps);
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    source.cols[0] = sourceLists;
    source.size = 2;

    sourceLists.offsets[0] = 1;
    sourceLists.lengths[0] = 1;
    sourceLists.offsets[1] = 3;
    sourceLists.lengths[1] = 1;
    sourceLists.childCount = 4;
    sourceMaps.offsets[1] = 2;
    sourceMaps.lengths[1] = 2;
    sourceMaps.offsets[3] = 6;
    sourceMaps.lengths[3] = 1;
    sourceMaps.childCount = 7;
    sourceKeys.setVal(2, bytes("a"));
    sourceKeys.setVal(3, bytes("b"));
    sourceKeys.setVal(6, bytes("c"));
    sourceValues.vector[2] = 10;
    sourceValues.vector[3] = 20;
    sourceValues.vector[6] = 30;

    BytesColumnVector resultKeys = new BytesColumnVector();
    LongColumnVector resultValues = new LongColumnVector();
    MapColumnVector resultMaps =
        new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, resultKeys, resultValues);
    ListColumnVector resultLists =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, resultMaps);
    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = resultLists;
    roundTrip(source, new int[] {0}, result);

    assertEquals(2, resultLists.childCount);
    assertEquals(0, resultLists.offsets[0]);
    assertEquals(1, resultLists.lengths[0]);
    assertEquals(1, resultLists.offsets[1]);
    assertEquals(1, resultLists.lengths[1]);
    assertEquals(3, resultMaps.childCount);
    assertEquals(0, resultMaps.offsets[0]);
    assertEquals(2, resultMaps.lengths[0]);
    assertEquals(2, resultMaps.offsets[1]);
    assertEquals(1, resultMaps.lengths[1]);
    assertBytes("a", resultKeys, 0);
    assertBytes("b", resultKeys, 1);
    assertBytes("c", resultKeys, 2);
    assertEquals(10, resultValues.vector[0]);
    assertEquals(20, resultValues.vector[1]);
    assertEquals(30, resultValues.vector[2]);
  }

  @Test
  public void testSelectedRowsAndColumnMapAreCompacted() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(3);
    LongColumnVector ignored = new LongColumnVector();
    BytesColumnVector strings = new BytesColumnVector();
    LongColumnVector longs = new LongColumnVector();
    source.cols[0] = ignored;
    source.cols[1] = strings;
    source.cols[2] = longs;

    strings.initBuffer();
    strings.setVal(2, bytes("two"));
    strings.setVal(5, bytes("five"));
    strings.setVal(9, bytes("nine"));
    longs.vector[2] = 20;
    longs.vector[5] = 50;
    longs.vector[9] = 90;
    source.selectedInUse = true;
    source.selected[0] = 2;
    source.selected[1] = 5;
    source.selected[2] = 9;
    source.size = 3;

    VectorizedRowBatch result = new VectorizedRowBatch(2);
    result.cols[0] = new LongColumnVector();
    result.cols[1] = new BytesColumnVector();
    roundTrip(source, new int[] {2, 1}, result);

    assertEquals(3, result.size);
    assertFalse(result.selectedInUse);
    assertEquals(20, ((LongColumnVector) result.cols[0]).vector[0]);
    assertEquals(50, ((LongColumnVector) result.cols[0]).vector[1]);
    assertEquals(90, ((LongColumnVector) result.cols[0]).vector[2]);
    assertBytes("two", (BytesColumnVector) result.cols[1], 0);
    assertBytes("five", (BytesColumnVector) result.cols[1], 1);
    assertBytes("nine", (BytesColumnVector) result.cols[1], 2);
  }

  @Test
  public void testNullAndRepeatingColumnsUseLogicalRepresentation() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(3);
    LongColumnVector repeating = new LongColumnVector();
    DoubleColumnVector nullable = new DoubleColumnVector();
    BytesColumnVector repeatingNull = new BytesColumnVector();
    source.cols[0] = repeating;
    source.cols[1] = nullable;
    source.cols[2] = repeatingNull;
    source.size = 4;

    repeating.isRepeating = true;
    repeating.vector[0] = 73;
    nullable.noNulls = false;
    nullable.vector[0] = 1.25;
    nullable.isNull[1] = true;
    nullable.vector[2] = 2.5;
    nullable.vector[3] = 5.0;
    repeatingNull.isRepeating = true;
    repeatingNull.noNulls = false;
    repeatingNull.isNull[0] = true;

    VectorizedRowBatch result = new VectorizedRowBatch(3);
    result.cols[0] = new LongColumnVector();
    result.cols[1] = new DoubleColumnVector();
    result.cols[2] = new BytesColumnVector();
    roundTrip(source, new int[] {0, 1, 2}, result);

    LongColumnVector resultRepeating = (LongColumnVector) result.cols[0];
    assertTrue(resultRepeating.isRepeating);
    assertEquals(73, resultRepeating.vector[0]);
    DoubleColumnVector resultNullable = (DoubleColumnVector) result.cols[1];
    assertFalse(resultNullable.noNulls);
    assertTrue(resultNullable.isNull[1]);
    assertEquals(2.5, resultNullable.vector[2], 0);
    assertTrue(result.cols[2].isRepeating);
    assertFalse(result.cols[2].noNulls);
    assertTrue(result.cols[2].isNull[0]);
  }

  @Test
  public void testAdditionalPrimitivePhysicalVectors() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(5);
    DateColumnVector date = new DateColumnVector().setUsingProlepticCalendar(true);
    TimestampColumnVector timestamp = new TimestampColumnVector().setUsingProlepticCalendar(true);
    IntervalDayTimeColumnVector interval = new IntervalDayTimeColumnVector();
    DecimalColumnVector decimal = new DecimalColumnVector(12, 3);
    Decimal64ColumnVector decimal64 = new Decimal64ColumnVector(12, 3);
    source.cols[0] = date;
    source.cols[1] = timestamp;
    source.cols[2] = interval;
    source.cols[3] = decimal;
    source.cols[4] = decimal64;
    source.size = 1;

    date.vector[0] = -100000;
    timestamp.setIsUTC(true);
    timestamp.time[0] = 123456789L;
    timestamp.nanos[0] = 987654321;
    interval.set(0, new HiveIntervalDayTime(123, 456));
    decimal.set(0, HiveDecimal.create("123.456"));
    decimal64.set(0, HiveDecimal.create("789.012"));

    VectorizedRowBatch result = new VectorizedRowBatch(5);
    result.cols[0] = new DateColumnVector();
    result.cols[1] = new TimestampColumnVector();
    result.cols[2] = new IntervalDayTimeColumnVector();
    result.cols[3] = new DecimalColumnVector(12, 3);
    result.cols[4] = new Decimal64ColumnVector(12, 3);
    roundTrip(source, new int[] {0, 1, 2, 3, 4}, result);

    assertTrue(((DateColumnVector) result.cols[0]).isUsingProlepticCalendar());
    assertEquals(-100000, ((DateColumnVector) result.cols[0]).vector[0]);
    TimestampColumnVector resultTimestamp = (TimestampColumnVector) result.cols[1];
    assertTrue(resultTimestamp.isUTC());
    assertTrue(resultTimestamp.usingProlepticCalendar());
    assertEquals(123456789L, resultTimestamp.time[0]);
    assertEquals(987654321, resultTimestamp.nanos[0]);
    HiveIntervalDayTime resultInterval =
        ((IntervalDayTimeColumnVector) result.cols[2]).asScratchIntervalDayTime(0);
    assertEquals(123, resultInterval.getTotalSeconds());
    assertEquals(456, resultInterval.getNanos());
    assertEquals(HiveDecimal.create("123.456"),
        ((DecimalColumnVector) result.cols[3]).vector[0].getHiveDecimal());
    assertEquals(((Decimal64ColumnVector) source.cols[4]).vector[0],
        ((Decimal64ColumnVector) result.cols[4]).vector[0]);
  }

  @Test
  public void testListColumnIsCompacted() throws Exception {
    LongColumnVector sourceChild = new LongColumnVector();
    ListColumnVector sourceList = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, sourceChild);
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    source.cols[0] = sourceList;
    source.size = 2;
    source.selectedInUse = true;
    source.selected[0] = 2;
    source.selected[1] = 5;
    sourceList.offsets[2] = 3;
    sourceList.lengths[2] = 2;
    sourceChild.vector[3] = 30;
    sourceChild.vector[4] = 40;
    sourceList.offsets[5] = 8;
    sourceList.lengths[5] = 1;
    sourceChild.vector[8] = 80;

    ListColumnVector resultList =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector());
    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = resultList;
    roundTrip(source, new int[] {0}, result);

    assertEquals(3, resultList.childCount);
    assertEquals(0, resultList.offsets[0]);
    assertEquals(2, resultList.lengths[0]);
    assertEquals(2, resultList.offsets[1]);
    assertEquals(1, resultList.lengths[1]);
    assertEquals(30, ((LongColumnVector) resultList.child).vector[0]);
    assertEquals(40, ((LongColumnVector) resultList.child).vector[1]);
    assertEquals(80, ((LongColumnVector) resultList.child).vector[2]);
  }

  @Test
  public void testSimpleStructWithPrimitiveFields() throws Exception {
    BytesColumnVector strings = new BytesColumnVector();
    strings.initBuffer();
    LongColumnVector longs = new LongColumnVector();
    StructColumnVector struct =
        new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, strings, longs);
    VectorizedRowBatch source = batchWithColumn(struct, 2);
    strings.setVal(0, bytes("zero"));
    strings.setVal(1, bytes("one"));
    longs.vector[0] = 10;
    longs.vector[1] = 11;

    StructColumnVector result = structOf(new BytesColumnVector(), new LongColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertBytes("zero", (BytesColumnVector) result.fields[0], 0);
    assertBytes("one", (BytesColumnVector) result.fields[0], 1);
    assertEquals(10, ((LongColumnVector) result.fields[1]).vector[0]);
    assertEquals(11, ((LongColumnVector) result.fields[1]).vector[1]);
  }

  @Test
  public void testNullableStructParentOmitsInactiveFieldValues() throws Exception {
    LongColumnVector field = new LongColumnVector();
    StructColumnVector struct = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, field);
    VectorizedRowBatch source = batchWithColumn(struct, 3);
    struct.noNulls = false;
    struct.isNull[1] = true;
    field.vector[0] = 10;
    field.vector[1] = 999;
    field.vector[2] = 30;

    StructColumnVector result = structOf(new LongColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertTrue(result.isNull[1]);
    assertEquals(10, ((LongColumnVector) result.fields[0]).vector[0]);
    assertEquals(30, ((LongColumnVector) result.fields[0]).vector[2]);
  }

  @Test
  public void testSelectedStructRowsAreCompacted() throws Exception {
    LongColumnVector field = new LongColumnVector();
    StructColumnVector struct = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, field);
    VectorizedRowBatch source = batchWithColumn(struct, 3);
    source.selectedInUse = true;
    source.selected[0] = 2;
    source.selected[1] = 5;
    source.selected[2] = 9;
    struct.noNulls = false;
    struct.isNull[5] = true;
    field.vector[2] = 20;
    field.vector[5] = 999;
    field.vector[9] = 90;

    StructColumnVector result = structOf(new LongColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertEquals(20, ((LongColumnVector) result.fields[0]).vector[0]);
    assertTrue(result.isNull[1]);
    assertEquals(90, ((LongColumnVector) result.fields[0]).vector[2]);
  }

  @Test
  public void testNullableStructFieldsRetainIndependentNulls() throws Exception {
    LongColumnVector first = new LongColumnVector();
    DoubleColumnVector second = new DoubleColumnVector();
    StructColumnVector struct =
        new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, first, second);
    VectorizedRowBatch source = batchWithColumn(struct, 2);
    first.noNulls = false;
    first.isNull[0] = true;
    first.vector[1] = 12;
    second.noNulls = false;
    second.vector[0] = 2.5;
    second.isNull[1] = true;

    StructColumnVector result = structOf(new LongColumnVector(), new DoubleColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertTrue(result.fields[0].isNull[0]);
    assertEquals(12, ((LongColumnVector) result.fields[0]).vector[1]);
    assertEquals(2.5, ((DoubleColumnVector) result.fields[1]).vector[0], 0);
    assertTrue(result.fields[1].isNull[1]);
  }

  @Test
  public void testRepeatingNonNullAndNullStructs() throws Exception {
    LongColumnVector nonNullField = new LongColumnVector();
    StructColumnVector nonNull =
        new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, nonNullField);
    nonNull.isRepeating = true;
    nonNullField.vector[0] = 77;
    StructColumnVector nullStruct = structOf(new LongColumnVector());
    nullStruct.isRepeating = true;
    nullStruct.noNulls = false;
    nullStruct.isNull[0] = true;
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    source.cols[0] = nonNull;
    source.cols[1] = nullStruct;
    source.size = 4;

    StructColumnVector resultNonNull = structOf(new LongColumnVector());
    StructColumnVector resultNull = structOf(new LongColumnVector());
    VectorizedRowBatch result = new VectorizedRowBatch(2);
    result.cols[0] = resultNonNull;
    result.cols[1] = resultNull;
    roundTrip(source, new int[] {0, 1}, result);

    assertTrue(resultNonNull.isRepeating);
    assertEquals(77, ((LongColumnVector) resultNonNull.fields[0]).vector[0]);
    assertTrue(resultNull.isRepeating);
    assertTrue(resultNull.isNull[0]);
  }

  @Test
  public void testRepeatingStructChildFieldIsPreserved() throws Exception {
    LongColumnVector field = new LongColumnVector();
    field.isRepeating = true;
    field.vector[0] = 44;
    StructColumnVector struct = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, field);
    VectorizedRowBatch source = batchWithColumn(struct, 3);

    StructColumnVector result = structOf(new LongColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertTrue(result.fields[0].isRepeating);
    assertEquals(44, ((LongColumnVector) result.fields[0]).vector[0]);
  }

  @Test
  public void testNestedStructsWithIndependentNulls() throws Exception {
    LongColumnVector nestedLong = new LongColumnVector();
    BytesColumnVector nestedBytes = new BytesColumnVector();
    nestedBytes.initBuffer();
    StructColumnVector inner = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        nestedLong, nestedBytes);
    DoubleColumnVector outerDouble = new DoubleColumnVector();
    StructColumnVector outer = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        inner, outerDouble);
    VectorizedRowBatch source = batchWithColumn(outer, 3);
    outer.noNulls = false;
    outer.isNull[1] = true;
    inner.noNulls = false;
    inner.isNull[2] = true;
    nestedLong.vector[0] = 10;
    nestedBytes.setVal(0, bytes("ten"));
    outerDouble.vector[0] = 1.5;
    outerDouble.vector[2] = 3.5;

    StructColumnVector resultInner = structOf(new LongColumnVector(), new BytesColumnVector());
    StructColumnVector resultOuter = structOf(resultInner, new DoubleColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(resultOuter, 0));

    assertTrue(resultOuter.isNull[1]);
    assertTrue(resultInner.isNull[2]);
    assertEquals(10, ((LongColumnVector) resultInner.fields[0]).vector[0]);
    assertBytes("ten", (BytesColumnVector) resultInner.fields[1], 0);
    assertEquals(3.5, ((DoubleColumnVector) resultOuter.fields[1]).vector[2], 0);
  }

  @Test
  public void testStructContainingListAndMapUsesScatteredParentPositions() throws Exception {
    LongColumnVector listChild = new LongColumnVector();
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, listChild);
    BytesColumnVector mapKeys = new BytesColumnVector();
    mapKeys.initBuffer();
    LongColumnVector mapValues = new LongColumnVector();
    MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, mapKeys, mapValues);
    StructColumnVector struct = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, list, map);
    VectorizedRowBatch source = batchWithColumn(struct, 3);
    struct.noNulls = false;
    struct.isNull[1] = true;
    list.offsets[0] = 3;
    list.lengths[0] = 1;
    listChild.vector[3] = 30;
    list.offsets[2] = 7;
    list.lengths[2] = 2;
    listChild.vector[7] = 70;
    listChild.vector[8] = 80;
    map.offsets[0] = 2;
    map.lengths[0] = 1;
    mapKeys.setVal(2, bytes("a"));
    mapValues.vector[2] = 1;
    map.offsets[2] = 5;
    map.lengths[2] = 1;
    mapKeys.setVal(5, bytes("c"));
    mapValues.vector[5] = 3;

    ListColumnVector resultList =
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector());
    MapColumnVector resultMap = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new BytesColumnVector(), new LongColumnVector());
    StructColumnVector result = structOf(resultList, resultMap);
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertEquals(0, resultList.offsets[0]);
    assertEquals(1, resultList.lengths[0]);
    assertEquals(1, resultList.offsets[2]);
    assertEquals(2, resultList.lengths[2]);
    assertEquals(80, ((LongColumnVector) resultList.child).vector[2]);
    assertEquals(0, resultMap.offsets[0]);
    assertEquals(1, resultMap.offsets[2]);
    assertBytes("c", (BytesColumnVector) resultMap.keys, 1);
  }

  @Test
  public void testListAndMapContainingStructs() throws Exception {
    LongColumnVector listField = new LongColumnVector();
    StructColumnVector listStruct =
        new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, listField);
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, listStruct);
    list.offsets[0] = 2;
    list.lengths[0] = 2;
    listField.vector[2] = 20;
    listField.vector[3] = 30;
    BytesColumnVector mapKeys = new BytesColumnVector();
    mapKeys.initBuffer();
    LongColumnVector mapField = new LongColumnVector();
    StructColumnVector mapStruct =
        new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, mapField);
    MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, mapKeys, mapStruct);
    map.offsets[0] = 4;
    map.lengths[0] = 1;
    mapKeys.setVal(4, bytes("k"));
    mapField.vector[4] = 40;
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    source.cols[0] = list;
    source.cols[1] = map;
    source.size = 1;

    ListColumnVector resultList = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        structOf(new LongColumnVector()));
    MapColumnVector resultMap = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new BytesColumnVector(), structOf(new LongColumnVector()));
    VectorizedRowBatch result = new VectorizedRowBatch(2);
    result.cols[0] = resultList;
    result.cols[1] = resultMap;
    roundTrip(source, new int[] {0, 1}, result);

    LongColumnVector resultListField =
        (LongColumnVector) ((StructColumnVector) resultList.child).fields[0];
    assertEquals(20, resultListField.vector[0]);
    assertEquals(30, resultListField.vector[1]);
    assertBytes("k", (BytesColumnVector) resultMap.keys, 0);
    LongColumnVector resultMapField =
        (LongColumnVector) ((StructColumnVector) resultMap.values).fields[0];
    assertEquals(40, resultMapField.vector[0]);
  }

  @Test
  public void testAllNullStructHasZeroActiveChildren() throws Exception {
    LongColumnVector field = new LongColumnVector();
    StructColumnVector struct = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, field);
    VectorizedRowBatch source = batchWithColumn(struct, 2);
    struct.noNulls = false;
    struct.isNull[0] = true;
    struct.isNull[1] = true;
    field.vector[0] = 100;
    field.vector[1] = 200;

    StructColumnVector result = structOf(new LongColumnVector());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertTrue(result.isNull[0]);
    assertTrue(result.isNull[1]);
    assertEquals(0, ((LongColumnVector) result.fields[0]).vector[0]);
  }

  @Test
  public void testDirectUnionWithMixedPrimitiveFields() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn(),
        new DoubleColumnVector());
    union.tags[0] = 0;
    union.tags[1] = 1;
    union.tags[2] = 2;
    union.tags[3] = 0;
    ((LongColumnVector) union.fields[0]).vector[0] = 10;
    ((LongColumnVector) union.fields[0]).vector[3] = 40;
    ((BytesColumnVector) union.fields[1]).setVal(1, bytes("one"));
    ((DoubleColumnVector) union.fields[2]).vector[2] = 2.5;

    UnionColumnVector result = unionOf(new LongColumnVector(), bytesColumn(),
        new DoubleColumnVector());
    roundTrip(batchWithColumn(union, 4), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(0, result.tags[0]);
    assertEquals(1, result.tags[1]);
    assertEquals(2, result.tags[2]);
    assertEquals(0, result.tags[3]);
    assertEquals(10, ((LongColumnVector) result.fields[0]).vector[0]);
    assertBytes("one", (BytesColumnVector) result.fields[1], 1);
    assertEquals(2.5, ((DoubleColumnVector) result.fields[2]).vector[2], 0);
    assertEquals(40, ((LongColumnVector) result.fields[0]).vector[3]);
  }

  @Test
  public void testNullUnionRowsDoNotConsumeTagsOrChildren() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn());
    union.noNulls = false;
    union.isNull[1] = true;
    union.tags[0] = 0;
    union.tags[2] = 1;
    ((LongColumnVector) union.fields[0]).vector[0] = 11;
    ((BytesColumnVector) union.fields[1]).setVal(2, bytes("after-null"));

    UnionColumnVector result = unionOf(new LongColumnVector(), bytesColumn());
    roundTrip(batchWithColumn(union, 3), new int[] {0}, batchWithColumn(result, 0));

    assertFalse(result.isNull[0]);
    assertTrue(result.isNull[1]);
    assertFalse(result.isNull[2]);
    assertEquals(0, result.tags[0]);
    assertEquals(1, result.tags[2]);
    assertEquals(11, ((LongColumnVector) result.fields[0]).vector[0]);
    assertBytes("after-null", (BytesColumnVector) result.fields[1], 2);
  }

  @Test
  public void testSelectedUnionRowsAreCompacted() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn());
    union.tags[2] = 1;
    union.tags[5] = 0;
    ((BytesColumnVector) union.fields[1]).setVal(2, bytes("selected"));
    ((LongColumnVector) union.fields[0]).vector[5] = 55;
    VectorizedRowBatch source = batchWithColumn(union, 2);
    source.selectedInUse = true;
    source.selected[0] = 2;
    source.selected[1] = 5;

    UnionColumnVector result = unionOf(new LongColumnVector(), bytesColumn());
    roundTrip(source, new int[] {0}, batchWithColumn(result, 0));

    assertEquals(1, result.tags[0]);
    assertEquals(0, result.tags[1]);
    assertBytes("selected", (BytesColumnVector) result.fields[1], 0);
    assertEquals(55, ((LongColumnVector) result.fields[0]).vector[1]);
  }

  @Test
  public void testRepeatingUnion() throws Exception {
    UnionColumnVector nonNull = unionOf(new LongColumnVector(), bytesColumn());
    nonNull.isRepeating = true;
    nonNull.tags[0] = 1;
    ((BytesColumnVector) nonNull.fields[1]).setVal(0, bytes("repeat"));
    UnionColumnVector nullUnion = unionOf(new LongColumnVector());
    nullUnion.isRepeating = true;
    nullUnion.noNulls = false;
    nullUnion.isNull[0] = true;
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    source.cols[0] = nonNull;
    source.cols[1] = nullUnion;
    source.size = 4;

    UnionColumnVector resultNonNull = unionOf(new LongColumnVector(), bytesColumn());
    UnionColumnVector resultNull = unionOf(new LongColumnVector());
    VectorizedRowBatch result = new VectorizedRowBatch(2);
    result.cols[0] = resultNonNull;
    result.cols[1] = resultNull;
    roundTrip(source, new int[] {0, 1}, result);

    assertTrue(resultNonNull.isRepeating);
    assertEquals(1, resultNonNull.tags[0]);
    assertBytes("repeat", (BytesColumnVector) resultNonNull.fields[1], 0);
    assertTrue(resultNull.isRepeating);
    assertTrue(resultNull.isNull[0]);
  }

  @Test
  public void testUnionUnusedFieldHasZeroActiveRows() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn());
    union.tags[0] = 0;
    union.tags[1] = 0;
    ((LongColumnVector) union.fields[0]).vector[0] = 1;
    ((LongColumnVector) union.fields[0]).vector[1] = 2;

    UnionColumnVector result = unionOf(new LongColumnVector(), bytesColumn());
    roundTrip(batchWithColumn(union, 2), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(1, ((LongColumnVector) result.fields[0]).vector[0]);
    assertEquals(2, ((LongColumnVector) result.fields[0]).vector[1]);
    assertNull(((BytesColumnVector) result.fields[1]).vector[0]);
  }

  @Test
  public void testAllNullUnionHasNoTagsOrActiveChildren() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn());
    union.noNulls = false;
    union.isNull[0] = true;
    union.isNull[1] = true;
    ((LongColumnVector) union.fields[0]).vector[0] = 100;

    UnionColumnVector result = unionOf(new LongColumnVector(), bytesColumn());
    roundTrip(batchWithColumn(union, 2), new int[] {0}, batchWithColumn(result, 0));

    assertTrue(result.isNull[0]);
    assertTrue(result.isNull[1]);
    assertEquals(0, ((LongColumnVector) result.fields[0]).vector[0]);
  }

  @Test
  public void testStructContainingUnion() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector(), bytesColumn());
    union.tags[0] = 1;
    ((BytesColumnVector) union.fields[1]).setVal(0, bytes("nested"));
    StructColumnVector struct = structOf(union);

    UnionColumnVector resultUnion = unionOf(new LongColumnVector(), bytesColumn());
    roundTrip(batchWithColumn(struct, 1), new int[] {0},
        batchWithColumn(structOf(resultUnion), 0));

    assertEquals(1, resultUnion.tags[0]);
    assertBytes("nested", (BytesColumnVector) resultUnion.fields[1], 0);
  }

  @Test
  public void testUnionContainingStruct() throws Exception {
    StructColumnVector field = structOf(new LongColumnVector(), bytesColumn());
    UnionColumnVector union = unionOf(field, new DoubleColumnVector());
    union.tags[0] = 0;
    ((LongColumnVector) field.fields[0]).vector[0] = 7;
    ((BytesColumnVector) field.fields[1]).setVal(0, bytes("struct"));

    StructColumnVector resultField = structOf(new LongColumnVector(), bytesColumn());
    UnionColumnVector result = unionOf(resultField, new DoubleColumnVector());
    roundTrip(batchWithColumn(union, 1), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(0, result.tags[0]);
    assertEquals(7, ((LongColumnVector) resultField.fields[0]).vector[0]);
    assertBytes("struct", (BytesColumnVector) resultField.fields[1], 0);
  }

  @Test
  public void testListContainingUnion() throws Exception {
    UnionColumnVector child = unionOf(new LongColumnVector(), bytesColumn());
    child.tags[2] = 1;
    child.tags[3] = 0;
    ((BytesColumnVector) child.fields[1]).setVal(2, bytes("list-union"));
    ((LongColumnVector) child.fields[0]).vector[3] = 33;
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
    list.offsets[0] = 2;
    list.lengths[0] = 2;

    UnionColumnVector resultChild = unionOf(new LongColumnVector(), bytesColumn());
    ListColumnVector result = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, resultChild);
    roundTrip(batchWithColumn(list, 1), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(1, resultChild.tags[0]);
    assertEquals(0, resultChild.tags[1]);
    assertBytes("list-union", (BytesColumnVector) resultChild.fields[1], 0);
    assertEquals(33, ((LongColumnVector) resultChild.fields[0]).vector[1]);
  }

  @Test
  public void testUnionContainingList() throws Exception {
    LongColumnVector child = new LongColumnVector();
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
    list.offsets[1] = 3;
    list.lengths[1] = 2;
    child.vector[3] = 30;
    child.vector[4] = 40;
    BytesColumnVector firstField = bytesColumn();
    firstField.setVal(0, bytes("first"));
    UnionColumnVector union = unionOf(firstField, list);
    union.tags[1] = 1;

    ListColumnVector resultList = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector());
    UnionColumnVector result = unionOf(bytesColumn(), resultList);
    roundTrip(batchWithColumn(union, 2), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(1, result.tags[1]);
    assertEquals(2, resultList.lengths[1]);
    assertEquals(30, ((LongColumnVector) resultList.child).vector[0]);
    assertEquals(40, ((LongColumnVector) resultList.child).vector[1]);
  }

  @Test
  public void testMapValueContainingUnion() throws Exception {
    BytesColumnVector keys = bytesColumn();
    UnionColumnVector values = unionOf(new LongColumnVector(), bytesColumn());
    keys.setVal(2, bytes("key"));
    values.tags[2] = 1;
    ((BytesColumnVector) values.fields[1]).setVal(2, bytes("value"));
    MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, keys, values);
    map.offsets[0] = 2;
    map.lengths[0] = 1;

    UnionColumnVector resultValues = unionOf(new LongColumnVector(), bytesColumn());
    MapColumnVector result = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        bytesColumn(), resultValues);
    roundTrip(batchWithColumn(map, 1), new int[] {0}, batchWithColumn(result, 0));

    assertBytes("key", (BytesColumnVector) result.keys, 0);
    assertEquals(1, resultValues.tags[0]);
    assertBytes("value", (BytesColumnVector) resultValues.fields[1], 0);
  }

  @Test
  public void testUnionContainingMap() throws Exception {
    BytesColumnVector keys = bytesColumn();
    LongColumnVector values = new LongColumnVector();
    keys.setVal(4, bytes("map-key"));
    values.vector[4] = 44;
    MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, keys, values);
    map.offsets[1] = 4;
    map.lengths[1] = 1;
    UnionColumnVector union = unionOf(new LongColumnVector(), map);
    union.tags[1] = 1;

    MapColumnVector resultMap = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        bytesColumn(), new LongColumnVector());
    UnionColumnVector result = unionOf(new LongColumnVector(), resultMap);
    roundTrip(batchWithColumn(union, 2), new int[] {0}, batchWithColumn(result, 0));

    assertEquals(1, result.tags[1]);
    assertBytes("map-key", (BytesColumnVector) resultMap.keys, 0);
    assertEquals(44, ((LongColumnVector) resultMap.values).vector[0]);
  }

  @Test
  public void testNestedUnionContainingUnion() throws Exception {
    UnionColumnVector inner = unionOf(new LongColumnVector(), bytesColumn());
    inner.tags[0] = 1;
    ((BytesColumnVector) inner.fields[1]).setVal(0, bytes("inner"));
    UnionColumnVector outer = unionOf(inner, new DoubleColumnVector());
    outer.tags[0] = 0;

    UnionColumnVector resultInner = unionOf(new LongColumnVector(), bytesColumn());
    UnionColumnVector resultOuter = unionOf(resultInner, new DoubleColumnVector());
    roundTrip(batchWithColumn(outer, 1), new int[] {0}, batchWithColumn(resultOuter, 0));

    assertEquals(0, resultOuter.tags[0]);
    assertEquals(1, resultInner.tags[0]);
    assertBytes("inner", (BytesColumnVector) resultInner.fields[1], 0);
  }

  @Test
  public void testSerializerRejectsNegativeUnionTag() {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = -1;
    assertThrows(IllegalArgumentException.class,
        () -> serializer.serialize(batchWithColumn(union, 1), new int[] {0}, new BytesWritable()));
  }

  @Test
  public void testSerializerRejectsUnionTagBeyondFieldCount() {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = 1;
    assertThrows(IllegalArgumentException.class,
        () -> serializer.serialize(batchWithColumn(union, 1), new int[] {0}, new BytesWritable()));
  }

  @Test
  public void testDeserializerRejectsInvalidEncodedUnionTag() {
    BytesWritable invalid = new BytesWritable(new byte[] {1, 1, 0, 1});
    assertThrows(IOException.class, () -> deserializer.deserialize(invalid,
        batchWithColumn(unionOf(new LongColumnVector()), 0)));
  }

  @Test
  public void testDeserializerRejectsTruncatedUnionTagSequence() {
    BytesWritable truncated = new BytesWritable(new byte[] {2, 1, 0, 0});
    assertThrows(IOException.class, () -> deserializer.deserialize(truncated,
        batchWithColumn(unionOf(new LongColumnVector()), 0)));
  }

  @Test
  public void testDeserializerRejectsTruncatedUnionChildPayload() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = 0;
    ((LongColumnVector) union.fields[0]).vector[0] = 9;
    BytesWritable serialized = serialize(batchWithColumn(union, 1));
    byte[] truncatedBytes = Arrays.copyOf(
        serialized.getBytes(), serialized.getLength() - 1);

    assertThrows(IOException.class,
        () -> deserializer.deserialize(new BytesWritable(truncatedBytes),
            batchWithColumn(unionOf(new LongColumnVector()), 0)));
  }

  @Test
  public void testDeserializerRejectsTrailingUnionBytes() throws Exception {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = 0;
    ((LongColumnVector) union.fields[0]).vector[0] = 9;
    BytesWritable serialized = serialize(batchWithColumn(union, 1));
    byte[] trailingBytes = Arrays.copyOf(
        serialized.getBytes(), serialized.getLength() + 1);

    assertThrows(IOException.class,
        () -> deserializer.deserialize(new BytesWritable(trailingBytes),
            batchWithColumn(unionOf(new LongColumnVector()), 0)));
  }

  @Test
  public void testStructPayloadRejectsTrailingAndMalformedBytes() throws Exception {
    StructColumnVector struct = structOf(new LongColumnVector());
    ((LongColumnVector) struct.fields[0]).vector[0] = 7;
    VectorizedRowBatch source = batchWithColumn(struct, 1);
    BytesWritable serialized = new BytesWritable();
    serializer.serialize(source, new int[] {0}, serialized);

    byte[] trailingBytes = new byte[serialized.getLength() + 1];
    System.arraycopy(serialized.getBytes(), 0, trailingBytes, 0, serialized.getLength());
    BytesWritable trailing = new BytesWritable(trailingBytes);
    assertThrows(IOException.class,
        () -> deserializer.deserialize(trailing,
            batchWithColumn(structOf(new LongColumnVector()), 0)));

    byte[] malformedBytes = new byte[serialized.getLength() - 1];
    System.arraycopy(serialized.getBytes(), 0, malformedBytes, 0, malformedBytes.length);
    BytesWritable malformed = new BytesWritable(malformedBytes);
    assertThrows(IOException.class,
        () -> deserializer.deserialize(malformed,
            batchWithColumn(structOf(new LongColumnVector()), 0)));
  }

  private static RandomBatchLayout randomBatchLayout(Random random, int iteration) {
    final int size;
    switch (iteration) {
    case 0:
      size = 0;
      break;
    case 1:
      size = 1;
      break;
    case 2:
      size = 2;
      break;
    case 3:
      size = VectorizedRowBatch.DEFAULT_SIZE - 1;
      break;
    case 4:
      size = VectorizedRowBatch.DEFAULT_SIZE;
      break;
    default:
      size = random.nextInt(VectorizedRowBatch.DEFAULT_SIZE + 1);
      break;
    }

    // Force one selected scenario, then use selections often enough to exercise compaction.
    boolean selectedInUse = iteration == 2 || iteration > 4 && random.nextInt(4) == 0;
    int[] selected = null;
    if (selectedInUse) {
      selected = shuffledIndices(random);
    }
    return new RandomBatchLayout(size, selected);
  }

  private static RandomColumnLayout randomColumnLayout(Random random, int size, int iteration) {
    switch (iteration) {
    case 0:
    case 3:
      return RandomColumnLayout.nonNull(size, false);
    case 1:
      return RandomColumnLayout.nonNull(size, true);
    case 2:
      return RandomColumnLayout.randomNulls(random, size, false);
    case 4:
      return RandomColumnLayout.allNull(size, true);
    case 5:
      return RandomColumnLayout.allNull(size, false);
    default:
      boolean repeating = random.nextInt(10) == 0;
      if (random.nextInt(10) == 0) {
        return RandomColumnLayout.allNull(size, repeating);
      }
      if (random.nextInt(4) == 0) {
        return RandomColumnLayout.randomNulls(random, size, repeating);
      }
      return RandomColumnLayout.nonNull(size, repeating);
    }
  }

  private static int[] shuffledIndices(Random random) {
    int[] indices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    for (int index = 0; index < indices.length; index++) {
      indices[index] = index;
    }
    for (int index = indices.length - 1; index > 0; index--) {
      int other = random.nextInt(index + 1);
      int value = indices[index];
      indices[index] = indices[other];
      indices[other] = value;
    }
    return indices;
  }

  private <T> void assertRandomColumnRoundTrips(String typeName, long seed, int iterations,
      RandomColumnAdapter<T> adapter) throws Exception {
    Random random = new Random(seed);
    try (RandomColumnLog<T> output = newColumnLog(typeName, seed, adapter)) {
      for (int iteration = 0; iteration < iterations; iteration++) {
        RandomColumnScenario scenario = randomColumnScenario(random, iteration);
        List<T> expected = randomValues(random, scenario, adapter);
        output.write(iteration, scenario, expected);
        ColumnVector sourceColumn = adapter.createVector();
        scenario.columnLayout.apply(sourceColumn, scenario.batchLayout);
        for (int logical = 0; logical < scenario.valueCount(); logical++) {
          if (!scenario.isNull(logical)) {
            adapter.setValue(sourceColumn, scenario.sourceIndex(logical), expected.get(logical));
          }
        }

        VectorizedRowBatch result = batchWithColumn(adapter.createVector(), 0);
        roundTrip(scenario.batchLayout.batchWithColumn(sourceColumn), new int[] {0}, result);
        assertColumnRoundTrip(expected, scenario, result, adapter,
            typeName + ", seed=" + seed + ", iteration=" + iteration + ", " + scenario);
      }
    }
  }

  private static RandomColumnScenario randomColumnScenario(Random random, int iteration) {
    RandomBatchLayout batchLayout = randomBatchLayout(random, iteration);
    return new RandomColumnScenario(batchLayout,
        randomColumnLayout(random, batchLayout.size, iteration));
  }

  private static <T> List<T> randomValues(Random random, RandomColumnScenario scenario,
      RandomColumnAdapter<T> adapter) {
    List<T> values = new ArrayList<>(scenario.size());
    T previous = null;
    for (int logical = 0; logical < scenario.size(); logical++) {
      T value = scenario.columnLayout.isRepeating && logical > 0
          ? values.get(0) : adapter.randomValue(random, previous, logical);
      values.add(value);
      previous = value;
    }
    return values;
  }

  private static <T> void assertColumnRoundTrip(List<T> expected, RandomColumnScenario scenario,
      VectorizedRowBatch result, RandomColumnAdapter<T> adapter, String context) {
    assertEquals(context, expected.size(), result.size);
    assertFalse(context, result.selectedInUse);
    ColumnVector actual = result.cols[0];
    assertEquals(context, scenario.columnLayout.isRepeating, actual.isRepeating);
    for (int logical = 0; logical < expected.size(); logical++) {
      int actualIndex = actual.isRepeating ? 0 : logical;
      boolean actualIsNull = !actual.noNulls && actual.isNull[actualIndex];
      assertEquals(context + ", logicalRow=" + logical, scenario.isNull(logical),
          actualIsNull);
      if (!actualIsNull) {
        adapter.assertValueEquals(context + ", logicalRow=" + logical, expected.get(logical),
            actual, actualIndex);
      }
    }
    adapter.assertColumnInvariants(context, expected, scenario, actual);
  }

  private static RandomColumnAdapter<Long> bigIntAdapter() {
    return new RandomColumnAdapter<Long>() {
      @Override
      public ColumnVector createVector() {
        return new LongColumnVector();
      }

      @Override
      public Long randomValue(Random random, Long previous, int logical) {
        if (logical == 0) {
          return Long.MIN_VALUE;
        } else if (logical == 1) {
          return 0L;
        } else if (logical == 2) {
          return Long.MAX_VALUE;
        }
        return randomBigInt(random, previous);
      }

      @Override
      public void setValue(ColumnVector vector, int index, Long value) {
        ((LongColumnVector) vector).vector[index] = value;
      }

      @Override
      public void assertValueEquals(String context, Long expected, ColumnVector vector, int index) {
        assertEquals(context, expected.longValue(), ((LongColumnVector) vector).vector[index]);
      }

      @Override
      public String formatValue(Long value) {
        return String.valueOf(value);
      }
    };
  }

  private static RandomColumnAdapter<byte[]> stringAdapter() {
    return new RandomColumnAdapter<byte[]>() {
      @Override
      public ColumnVector createVector() {
        BytesColumnVector vector = new BytesColumnVector();
        vector.initBuffer();
        return vector;
      }

      @Override
      public byte[] randomValue(Random random, byte[] previous, int logical) {
        if (logical < INTERESTING_STRINGS.length) {
          return INTERESTING_STRINGS[logical];
        }
        return randomString(random, previous);
      }

      @Override
      public void setValue(ColumnVector vector, int index, byte[] value) {
        ((BytesColumnVector) vector).setVal(index, value);
      }

      @Override
      public void assertValueEquals(String context, byte[] expected, ColumnVector vector,
          int index) {
        BytesColumnVector actual = (BytesColumnVector) vector;
        assertArrayEquals(context, expected, Arrays.copyOfRange(actual.vector[index],
            actual.start[index], actual.start[index] + actual.length[index]));
      }

      @Override
      public String formatValue(byte[] value) {
        return formatUtf8String(value);
      }
    };
  }

  private static RandomColumnAdapter<HiveDecimal> decimalAdapter() {
    return new RandomColumnAdapter<HiveDecimal>() {
      @Override
      public ColumnVector createVector() {
        return new DecimalColumnVector(DECIMAL_PRECISION, DECIMAL_SCALE);
      }

      @Override
      public HiveDecimal randomValue(Random random, HiveDecimal previous, int logical) {
        if (logical < INTERESTING_DECIMALS.length) {
          return INTERESTING_DECIMALS[logical];
        }
        return randomDecimal(random, previous);
      }

      @Override
      public void setValue(ColumnVector vector, int index, HiveDecimal value) {
        ((DecimalColumnVector) vector).set(index, value);
      }

      @Override
      public void assertValueEquals(String context, HiveDecimal expected, ColumnVector vector,
          int index) {
        assertEquals(context, expected,
            ((DecimalColumnVector) vector).vector[index].getHiveDecimal());
      }

      @Override
      public String formatValue(HiveDecimal value) {
        return value.toString();
      }
    };
  }

  private static <T> RandomColumnAdapter<ArrayValue<T>> arrayAdapter(
      RandomColumnAdapter<T> childAdapter) {
    return new RandomColumnAdapter<ArrayValue<T>>() {
      @Override
      public ColumnVector createVector() {
        return new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, childAdapter.createVector());
      }

      @Override
      public ArrayValue<T> randomValue(Random random, ArrayValue<T> previous, int logical) {
        if (previous != null && random.nextInt(10) == 0) {
          return previous;
        }
        int choice = random.nextInt(100);
        int length = choice < 15 ? 0 : choice < 25 ? 1 : random.nextInt(MAX_ARRAY_LENGTH + 1);
        List<NullableValue<T>> elements = new ArrayList<>(length);
        T previousElement = null;
        for (int element = 0; element < length; element++) {
          boolean isNull = random.nextInt(5) == 0;
          T value = isNull ? null : childAdapter.randomValue(random, previousElement, element);
          elements.add(new NullableValue<>(isNull, value));
          if (!isNull) {
            previousElement = value;
          }
        }
        return new ArrayValue<>(random.nextInt(4), elements);
      }

      @Override
      public void setValue(ColumnVector vector, int index, ArrayValue<T> value) {
        ListColumnVector list = (ListColumnVector) vector;
        int offset = list.childCount + value.gapBefore;
        list.offsets[index] = offset;
        list.lengths[index] = value.elements.size();
        list.child.ensureSize(offset + value.elements.size(), true);
        for (int element = 0; element < value.elements.size(); element++) {
          NullableValue<T> expected = value.elements.get(element);
          int childIndex = offset + element;
          if (expected.isNull) {
            setNull(list.child, childIndex);
          } else {
            childAdapter.setValue(list.child, childIndex, expected.value);
          }
        }
        list.childCount = offset + value.elements.size();
      }

      @Override
      public void assertValueEquals(String context, ArrayValue<T> expected, ColumnVector vector,
          int index) {
        ListColumnVector list = (ListColumnVector) vector;
        assertEquals(context, expected.elements.size(), list.lengths[index]);
        int offset = Math.toIntExact(list.offsets[index]);
        for (int element = 0; element < expected.elements.size(); element++) {
          NullableValue<T> expectedElement = expected.elements.get(element);
          int childIndex = offset + element;
          boolean actualIsNull = isNull(list.child, childIndex);
          assertEquals(context + ", element=" + element, expectedElement.isNull, actualIsNull);
          if (!actualIsNull) {
            childAdapter.assertValueEquals(context + ", element=" + element,
                expectedElement.value, list.child, childIndex);
          }
        }
      }

      @Override
      public String formatValue(ArrayValue<T> value) {
        StringBuilder formatted = new StringBuilder("[");
        for (int element = 0; element < value.elements.size(); element++) {
          if (element > 0) {
            formatted.append(", ");
          }
          NullableValue<T> expected = value.elements.get(element);
          formatted.append(expected.isNull ? "NULL" : childAdapter.formatValue(expected.value));
        }
        return formatted.append(']').toString();
      }

      @Override
      public void assertColumnInvariants(String context, List<ArrayValue<T>> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        ListColumnVector list = (ListColumnVector) vector;
        int expectedChildCount = 0;
        for (int logical = 0; logical < scenario.valueCount(); logical++) {
          int index = list.isRepeating ? 0 : logical;
          assertEquals(context + ", logicalRow=" + logical, expectedChildCount,
              list.offsets[index]);
          int expectedLength = scenario.isNull(logical) ? 0 : expected.get(logical).elements.size();
          assertEquals(context + ", logicalRow=" + logical, expectedLength, list.lengths[index]);
          expectedChildCount += expectedLength;
        }
        assertEquals(context, expectedChildCount, list.childCount);
      }
    };
  }

  private static void setNull(ColumnVector vector, int index) {
    vector.noNulls = false;
    vector.isNull[index] = true;
  }

  private static boolean isNull(ColumnVector vector, int index) {
    return !vector.noNulls && vector.isNull[index];
  }

  private static long randomBigInt(Random random, Long previous) {
    int choice = random.nextInt(100);
    if (choice < 55) {
      return random.nextInt(21) - 10;
    } else if (choice < 70) {
      return INTERESTING_BIGINTS[random.nextInt(INTERESTING_BIGINTS.length)];
    } else if (choice < 90 || previous == null) {
      return random.nextLong();
    }
    return previous;
  }

  private static String formatUtf8String(byte[] value) {
    String decoded = new String(value, StandardCharsets.UTF_8);
    StringBuilder formatted = new StringBuilder("\"");
    for (int offset = 0; offset < decoded.length();) {
      int codePoint = decoded.codePointAt(offset);
      offset += Character.charCount(codePoint);
      switch (codePoint) {
      case '\\':
        formatted.append("\\\\");
        break;
      case '\"':
        formatted.append("\\\"");
        break;
      case '\n':
        formatted.append("\\n");
        break;
      case '\r':
        formatted.append("\\r");
        break;
      case '\t':
        formatted.append("\\t");
        break;
      default:
        if (Character.isISOControl(codePoint)) {
          formatted.append(String.format("\\u%04x", codePoint));
        } else {
          formatted.appendCodePoint(codePoint);
        }
        break;
      }
    }
    return formatted.append('\"').toString();
  }

  private static byte[] randomString(Random random, byte[] previous) {
    int choice = random.nextInt(100);
    if (choice < 20) {
      return INTERESTING_STRINGS[random.nextInt(INTERESTING_STRINGS.length)];
    } else if (choice >= 90 && previous != null) {
      return previous;
    }
    int length = choice < 85 ? random.nextInt(33) : 256 + random.nextInt(769);
    byte[] value = new byte[length];
    random.nextBytes(value);
    return value;
  }

  private static HiveDecimal randomDecimal(Random random, HiveDecimal previous) {
    int choice = random.nextInt(100);
    if (choice < 20) {
      return INTERESTING_DECIMALS[random.nextInt(INTERESTING_DECIMALS.length)];
    } else if (choice >= 90 && previous != null) {
      return previous;
    }
    BigInteger unscaled = choice < 60
        ? BigInteger.valueOf(random.nextInt(200001) - 100000)
        : new BigInteger(66, random).multiply(random.nextBoolean() ? BigInteger.ONE
            : BigInteger.valueOf(-1));
    return HiveDecimal.create(unscaled, DECIMAL_SCALE);
  }

  private static <T> RandomColumnLog<T> newColumnLog(String typeName, long seed,
      RandomColumnAdapter<T> adapter) throws IOException {
    Path directory = Paths.get(System.getProperty("test.tmp.dir", "target/tmp"));
    Files.createDirectories(directory);
    String fileName = COLUMN_LOG_PREFIX + typeName.replaceAll("[^A-Za-z0-9]+", "-") + ".log";
    BufferedWriter writer = Files.newBufferedWriter(directory.resolve(fileName),
        StandardCharsets.UTF_8);
    return new RandomColumnLog<T>() {
      @Override
      public void write(int iteration, RandomColumnScenario scenario, List<T> values)
          throws IOException {
        if (iteration >= MAX_LOGGED_PROPERTY_TEST_CASES) {
          return;
        }
        writer.write("seed=" + seed + ", iteration=" + iteration + ", " + scenario
            + ", values=[");
        for (int logical = 0; logical < values.size(); logical++) {
          if (logical > 0) {
            writer.write(", ");
          }
          writer.write(scenario.isNull(logical)
              ? "NULL" : adapter.formatValue(values.get(logical)));
        }
        writer.write("]");
        writer.newLine();
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }

  private interface RandomColumnLog<T> extends AutoCloseable {
    void write(int iteration, RandomColumnScenario scenario, List<T> values) throws IOException;

    @Override
    void close() throws IOException;
  }

  private interface RandomColumnAdapter<T> {
    ColumnVector createVector();

    T randomValue(Random random, T previous, int logical);

    void setValue(ColumnVector vector, int index, T value);

    void assertValueEquals(String context, T expected, ColumnVector vector, int index);

    String formatValue(T value);

    default void assertColumnInvariants(String context, List<T> expected,
        RandomColumnScenario scenario, ColumnVector vector) {
    }
  }

  private static final class NullableValue<T> {
    private final boolean isNull;
    private final T value;

    private NullableValue(boolean isNull, T value) {
      this.isNull = isNull;
      this.value = value;
    }
  }

  private static final class ArrayValue<T> {
    private final int gapBefore;
    private final List<NullableValue<T>> elements;

    private ArrayValue(int gapBefore, List<NullableValue<T>> elements) {
      this.gapBefore = gapBefore;
      this.elements = elements;
    }
  }

  private static final class RandomColumnScenario {
    private final RandomBatchLayout batchLayout;
    private final RandomColumnLayout columnLayout;

    private RandomColumnScenario(RandomBatchLayout batchLayout, RandomColumnLayout columnLayout) {
      this.batchLayout = batchLayout;
      this.columnLayout = columnLayout;
    }

    private int size() {
      return batchLayout.size;
    }

    private int valueCount() {
      return columnLayout.isRepeating ? Math.min(size(), 1) : size();
    }

    private boolean isNull(int logical) {
      return columnLayout.isNull(logical);
    }

    private int sourceIndex(int logical) {
      return columnLayout.sourceIndex(batchLayout, logical);
    }

    @Override
    public String toString() {
      return batchLayout + ", " + columnLayout;
    }
  }

  /** A reusable description of logical rows and their physical positions in a source batch. */
  private static final class RandomBatchLayout {
    private final int size;
    private final int[] selected;

    private RandomBatchLayout(int size, int[] selected) {
      this.size = size;
      this.selected = selected;
    }

    private int sourceIndex(int logical) {
      return selected == null ? logical : selected[logical];
    }

    private VectorizedRowBatch batchWithColumn(ColumnVector column) {
      VectorizedRowBatch batch = TestVectorShuffleBatchSerde.batchWithColumn(column, size);
      if (selected != null) {
        batch.selectedInUse = true;
        System.arraycopy(selected, 0, batch.selected, 0, size);
      }
      return batch;
    }

    @Override
    public String toString() {
      return "size=" + size + ", selectedInUse=" + (selected != null);
    }
  }

  /** A reusable description of repeating and null state for a column of any vector type. */
  private static final class RandomColumnLayout {
    private final boolean isRepeating;
    private final boolean[] nulls;

    private RandomColumnLayout(boolean isRepeating, boolean[] nulls) {
      this.isRepeating = isRepeating;
      this.nulls = nulls;
    }

    private static RandomColumnLayout nonNull(int size, boolean repeating) {
      return new RandomColumnLayout(repeating, new boolean[repeating ? Math.min(size, 1) : size]);
    }

    private static RandomColumnLayout allNull(int size, boolean repeating) {
      boolean[] nulls = new boolean[repeating ? Math.min(size, 1) : size];
      Arrays.fill(nulls, true);
      return new RandomColumnLayout(repeating, nulls);
    }

    private static RandomColumnLayout randomNulls(Random random, int size, boolean repeating) {
      boolean[] nulls = new boolean[repeating ? Math.min(size, 1) : size];
      for (int logical = 0; logical < nulls.length; logical++) {
        nulls[logical] = random.nextInt(5) == 0;
      }
      return new RandomColumnLayout(repeating, nulls);
    }

    private boolean isNull(int logical) {
      return nulls.length > 0 && nulls[isRepeating ? 0 : logical];
    }

    private int sourceIndex(RandomBatchLayout batchLayout, int logical) {
      return isRepeating ? 0 : batchLayout.sourceIndex(logical);
    }

    private void apply(ColumnVector column, RandomBatchLayout batchLayout) {
      column.isRepeating = isRepeating;
      for (int logical = 0; logical < batchLayout.size; logical++) {
        if (isNull(logical)) {
          column.noNulls = false;
          column.isNull[sourceIndex(batchLayout, logical)] = true;
        }
      }
    }

    @Override
    public String toString() {
      int nullCount = 0;
      for (boolean isNull : nulls) {
        nullCount += isNull ? 1 : 0;
      }
      return "isRepeating=" + isRepeating + ", nullCount=" + nullCount;
    }
  }

  private static VectorizedRowBatch batchWithColumn(ColumnVector column, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = column;
    batch.size = size;
    return batch;
  }

  private static BytesColumnVector bytesColumn() {
    BytesColumnVector bytes = new BytesColumnVector();
    bytes.initBuffer();
    return bytes;
  }

  private static StructColumnVector structOf(ColumnVector... fields) {
    return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  private static UnionColumnVector unionOf(ColumnVector... fields) {
    return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  private BytesWritable serialize(VectorizedRowBatch source) throws Exception {
    BytesWritable serialized = new BytesWritable();
    serializer.serialize(source, new int[] {0}, serialized);
    return serialized;
  }

  private void roundTrip(VectorizedRowBatch source, int[] sourceColumnMap,
      VectorizedRowBatch destination) throws Exception {
    BytesWritable serialized = new BytesWritable();
    serializer.serialize(source, sourceColumnMap, serialized);
    deserializer.deserialize(serialized, destination);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static void assertBytes(String expected, BytesColumnVector vector, int index) {
    assertArrayEquals(bytes(expected),
        Arrays.copyOfRange(vector.vector[index], vector.start[index],
            vector.start[index] + vector.length[index]));
  }
}
