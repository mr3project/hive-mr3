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

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchSerde {
  private final VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();
  private final VectorShuffleBatchDeserializer deserializer = new VectorShuffleBatchDeserializer();

  @Test
  public void testSelectedRowsAndColumnMapAreCompacted() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(3);
    LongColumnVector ignored = new LongColumnVector();
    BytesColumnVector strings = new BytesColumnVector();
    LongColumnVector longs = new LongColumnVector();
    source.cols[0] = ignored;
    source.cols[1] = strings;
    source.cols[2] = longs;

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
  public void testStructColumnIsRejectedUntilInactiveChildrenCanBeOmitted() {
    VectorizedRowBatch source = new VectorizedRowBatch(1);
    source.cols[0] = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new BytesColumnVector());
    source.size = 1;

    assertThrows(IllegalArgumentException.class,
        () -> serializer.serialize(source, new int[] {0}, new BytesWritable()));
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
