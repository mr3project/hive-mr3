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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchSerde {
  private final VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
  private final VectorShuffleBatchDeserializer deserializer = new VectorShuffleBatchDeserializer();

  @Test
  public void testBigIntColumnSchema() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    LongColumnVector sourceLongs = new LongColumnVector();
    source.cols[0] = sourceLongs;
    source.size = 3;
    sourceLongs.vector[0] = Long.MIN_VALUE;
    sourceLongs.vector[1] = 0;
    sourceLongs.vector[2] = Long.MAX_VALUE;

    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = new LongColumnVector();
    roundTrip(source, new int[] {0}, result);

    LongColumnVector resultLongs = (LongColumnVector) result.cols[0];
    assertEquals(Long.MIN_VALUE, resultLongs.vector[0]);
    assertEquals(0, resultLongs.vector[1]);
    assertEquals(Long.MAX_VALUE, resultLongs.vector[2]);
  }

  @Test
  public void testStringColumnSchema() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    BytesColumnVector sourceStrings = new BytesColumnVector();
    sourceStrings.initBuffer();
    source.cols[0] = sourceStrings;
    source.size = 3;
    sourceStrings.setVal(0, bytes("alpha"));
    sourceStrings.setVal(1, bytes(""));
    sourceStrings.setVal(2, bytes("omega"));

    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = new BytesColumnVector();
    roundTrip(source, new int[] {0}, result);

    BytesColumnVector resultStrings = (BytesColumnVector) result.cols[0];
    assertBytes("alpha", resultStrings, 0);
    assertBytes("", resultStrings, 1);
    assertBytes("omega", resultStrings, 2);
  }

  @Test
  public void testDecimalColumnSchema() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    DecimalColumnVector sourceDecimals = new DecimalColumnVector(20, 4);
    source.cols[0] = sourceDecimals;
    source.size = 3;
    sourceDecimals.set(0, HiveDecimal.create("-12345.6789"));
    sourceDecimals.set(1, HiveDecimal.ZERO);
    sourceDecimals.set(2, HiveDecimal.create("98765.4321"));

    VectorizedRowBatch result = new VectorizedRowBatch(1);
    result.cols[0] = new DecimalColumnVector(20, 4);
    roundTrip(source, new int[] {0}, result);

    DecimalColumnVector resultDecimals = (DecimalColumnVector) result.cols[0];
    assertEquals(HiveDecimal.create("-12345.6789"), resultDecimals.vector[0].getHiveDecimal());
    assertEquals(HiveDecimal.ZERO, resultDecimals.vector[1].getHiveDecimal());
    assertEquals(HiveDecimal.create("98765.4321"), resultDecimals.vector[2].getHiveDecimal());
  }

  @Test
  public void testArrayOfBigIntColumnSchema() throws Exception {
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
  public void testDirectAndNestedUnionColumnsRemainRejected() {
    VectorizedRowBatch direct = batchWithColumn(
        new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()), 1);
    assertThrows(IllegalArgumentException.class,
        () -> serializer.serialize(direct, new int[] {0}, new BytesWritable()));

    StructColumnVector struct = structOf(
        new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()));
    VectorizedRowBatch nested = batchWithColumn(struct, 1);
    assertThrows(IllegalArgumentException.class,
        () -> serializer.serialize(nested, new int[] {0}, new BytesWritable()));

    BytesWritable directPayload = new BytesWritable(new byte[] {1, 1, 0});
    assertThrows(IllegalArgumentException.class,
        () -> deserializer.deserialize(directPayload, batchWithColumn(
            new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()), 0)));
    BytesWritable nestedPayload = new BytesWritable(new byte[] {1, 1, 0, 0});
    assertThrows(IllegalArgumentException.class,
        () -> deserializer.deserialize(nestedPayload,
            batchWithColumn(structOf(new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                new LongColumnVector())), 0)));
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

  private static VectorizedRowBatch batchWithColumn(ColumnVector column, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = column;
    batch.size = size;
    return batch;
  }

  private static StructColumnVector structOf(ColumnVector... fields) {
    return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
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
        java.util.Arrays.copyOfRange(vector.vector[index], vector.start[index],
            vector.start[index] + vector.length[index]));
  }
}
