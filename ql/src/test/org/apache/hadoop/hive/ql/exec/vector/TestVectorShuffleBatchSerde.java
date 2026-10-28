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

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchSerde {
  private static final long BIGINT_PROPERTY_TEST_SEED = 0x5EEDB16L;
  private static final long STRING_PROPERTY_TEST_SEED = 0x5EED517L;
  private static final long DECIMAL_PROPERTY_TEST_SEED = 0x5EEDDEC1L;
  private static final long DOUBLE_PROPERTY_TEST_SEED = 0x5EEDD0B1L;
  private static final long DATE_PROPERTY_TEST_SEED = 0x5EEDDA7EL;
  private static final long TIMESTAMP_PROPERTY_TEST_SEED = 0x5EED715EL;
  private static final long INTERVAL_DAY_TIME_PROPERTY_TEST_SEED = 0x5EED1D7L;
  private static final long BYTES_SLICE_PROPERTY_TEST_SEED = 0x511CE517L;
  private static final long COLUMN_MAPPING_PROPERTY_TEST_SEED = 0xC01A4A9L;
  private static final long ARRAY_BIGINT_PROPERTY_TEST_SEED = 0xA22A7B16L;
  private static final long ARRAY_STRING_PROPERTY_TEST_SEED = 0xA22A7517L;
  private static final long ARRAY_DECIMAL_PROPERTY_TEST_SEED = 0xA22ADEC1L;
  private static final long NESTED_ARRAY_PROPERTY_TEST_SEED = 0xA22AA22AL;
  private static final long STRUCT_PROPERTY_TEST_SEED = 0x572AC7L;
  private static final long NESTED_STRUCT_PROPERTY_TEST_SEED = 0x572AC7A22AL;
  private static final long UNION_PROPERTY_TEST_SEED = 0xA110L;
  private static final long NESTED_UNION_PROPERTY_TEST_SEED = 0xA110A22AL;
  private static final long STRUCT_WITH_UNION_PROPERTY_TEST_SEED = 0x572A110L;
  private static final long UNION_WITH_STRUCT_PROPERTY_TEST_SEED = 0xA110572L;
  private static final long ARRAY_STRUCT_PROPERTY_TEST_SEED = 0xA22572L;
  private static final long ARRAY_UNION_PROPERTY_TEST_SEED = 0xA22A110L;
  private static final long MAP_PROPERTY_TEST_SEED = 0x6A4B16L;
  private static final long NESTED_MAP_PROPERTY_TEST_SEED = 0x6A4A22AL;
  private static final long COMPLEX_MAP_PROPERTY_TEST_SEED = 0x6A4572A110L;
  private static final long MAP_MAP_PROPERTY_TEST_SEED = 0x6A46A4L;
  private static final long ARRAY_MAP_PROPERTY_TEST_SEED = 0xA226A4L;
  private static final long STRUCT_MAP_PROPERTY_TEST_SEED = 0x5726A4L;
  private static final long UNION_MAP_PROPERTY_TEST_SEED = 0xA1106A4L;
  private static final int BIGINT_PROPERTY_TEST_ITERATIONS = 10000;
  private static final int PROPERTY_TEST_ITERATIONS = 200;
  private static final int BYTES_SLICE_PROPERTY_TEST_ITERATIONS = 200;
  private static final int COLUMN_MAPPING_PROPERTY_TEST_ITERATIONS = 200;
  private static final int ARRAY_PROPERTY_TEST_ITERATIONS = 1000;
  private static final int NESTED_ARRAY_PROPERTY_TEST_ITERATIONS = 500;
  private static final int COMPLEX_PROPERTY_TEST_ITERATIONS = 500;
  private static final int NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS = 200;
  private static final int MAX_STRING_LENGTH = 24;
  private static final int MAX_ARRAY_LENGTH = 16;
  private static final int MAX_MAP_LENGTH = 16;
  private static final int SERIALIZE_BUFFER_SIZE = 1024 * 1024;
  private static final int DECIMAL_PRECISION = 20;
  private static final int DECIMAL_SCALE = 4;
  private static final int MAX_LOGGED_PROPERTY_TEST_CASES = 10;
  private static final String COLUMN_LOG_PREFIX = "TestVectorShuffleBatchSerde-";
  private static final byte[] ALPHANUMERIC =
      bytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");
  private static final long[] INTERESTING_BIGINTS = {
      Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE, -11, -10, -1, 0, 1, 10, 11,
      Integer.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE
  };
  private static final byte[][] INTERESTING_STRINGS = {
      bytes(""), bytes("a"), bytes("alpha"), bytes("with spaces"),
      bytes("special-_.:/@#"), bytes("quote\"slash\\")
  };
  private static final double[] INTERESTING_DOUBLES = {
      Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -Double.MIN_NORMAL, -Double.MIN_VALUE, -0.0d,
      0.0d, Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE, Double.POSITIVE_INFINITY,
      Double.NaN
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
  public void testDecimal64ToDecimalColumnSchema() throws Exception {
    HiveDecimal expected = HiveDecimal.create("1234567890.1234");
    Decimal64ColumnVector source = new Decimal64ColumnVector(DECIMAL_PRECISION, DECIMAL_SCALE);
    source.set(0, expected);
    DecimalColumnVector destination = new DecimalColumnVector(DECIMAL_PRECISION, DECIMAL_SCALE);

    roundTrip(batchWithColumn(source, 1), new int[] {0}, batchWithColumn(destination, 0));

    assertEquals(expected, destination.vector[0].getHiveDecimal());
  }

  @Test
  public void testDecimalToDecimal64ColumnSchema() throws Exception {
    HiveDecimal expected = HiveDecimal.create("1234567890.1234");
    DecimalColumnVector source = new DecimalColumnVector(DECIMAL_PRECISION, DECIMAL_SCALE);
    source.set(0, expected);
    Decimal64ColumnVector destination = new Decimal64ColumnVector(DECIMAL_PRECISION, DECIMAL_SCALE);

    roundTrip(batchWithColumn(source, 1), new int[] {0}, batchWithColumn(destination, 0));

    HiveDecimalWritable writable = new HiveDecimalWritable(expected);
    assertEquals(writable.serialize64(DECIMAL_SCALE), destination.vector[0]);
  }

  @Test
  public void testDoubleColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("DOUBLE", DOUBLE_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
        doubleAdapter());
  }

  @Test
  public void testDateColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("DATE_HYBRID", DATE_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
        dateAdapter(false));
    assertRandomColumnRoundTrips("DATE_PROLEPTIC", DATE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, dateAdapter(true));
  }

  @Test
  public void testTimestampColumnSchema() throws Exception {
    for (boolean isUtc : new boolean[] {false, true}) {
      for (boolean proleptic : new boolean[] {false, true}) {
        assertRandomColumnRoundTrips("TIMESTAMP_UTC_" + isUtc + "_PROLEPTIC_" + proleptic,
            TIMESTAMP_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
            timestampAdapter(isUtc, proleptic));
      }
    }
  }

  @Test
  public void testIntervalDayTimeColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("INTERVAL_DAY_TIME", INTERVAL_DAY_TIME_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, intervalDayTimeAdapter());
  }

  @Test
  public void testVoidColumnSchema() throws Exception {
    for (int size : new int[] {0, 1, 17, VectorizedRowBatch.DEFAULT_SIZE}) {
      VoidColumnVector source = new VoidColumnVector();
      source.noNulls = false;
      source.isRepeating = true;
      source.isNull[0] = true;
      VectorizedRowBatch destination = batchWithColumn(new VoidColumnVector(), 0);
      roundTrip(batchWithColumn(source, size), new int[] {0}, destination);
      assertEquals(size, destination.size);
      assertEquals(size == 0, destination.cols[0].noNulls);
      assertEquals(size > 0, destination.cols[0].isNull[0]);
    }
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
  public void testStructColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("STRUCT<BIGINT,STRING,DECIMAL>", STRUCT_PROPERTY_TEST_SEED,
        COMPLEX_PROPERTY_TEST_ITERATIONS,
        structAdapter(bigIntAdapter(), stringAdapter(), decimalAdapter()));
  }

  @Test
  public void testNestedStructColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("STRUCT<ARRAY<BIGINT>,STRING>", NESTED_STRUCT_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        structAdapter(arrayAdapter(bigIntAdapter()), stringAdapter()));
  }

  @Test
  public void testUnionColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("UNIONTYPE<BIGINT,STRING,DECIMAL>", UNION_PROPERTY_TEST_SEED,
        COMPLEX_PROPERTY_TEST_ITERATIONS,
        unionAdapter(bigIntAdapter(), stringAdapter(), decimalAdapter()));
  }

  @Test
  public void testNestedUnionColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("UNIONTYPE<ARRAY<BIGINT>,STRING>", NESTED_UNION_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        unionAdapter(arrayAdapter(bigIntAdapter()), stringAdapter()));
  }

  @Test
  public void testStructContainingUnionPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("STRUCT<UNIONTYPE<BIGINT,STRING>,ARRAY<DECIMAL>>",
        STRUCT_WITH_UNION_PROPERTY_TEST_SEED, NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        structAdapter(unionAdapter(bigIntAdapter(), stringAdapter()),
            arrayAdapter(decimalAdapter())));
  }

  @Test
  public void testUnionContainingStructPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("UNIONTYPE<STRUCT<BIGINT,STRING>,ARRAY<DECIMAL>>",
        UNION_WITH_STRUCT_PROPERTY_TEST_SEED, NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        unionAdapter(structAdapter(bigIntAdapter(), stringAdapter()),
            arrayAdapter(decimalAdapter())));
  }

  @Test
  public void testArrayContainingStructPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<STRUCT<BIGINT,STRING>>", ARRAY_STRUCT_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        arrayAdapter(structAdapter(bigIntAdapter(), stringAdapter())));
  }

  @Test
  public void testArrayContainingUnionPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<UNIONTYPE<BIGINT,STRING>>", ARRAY_UNION_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        arrayAdapter(unionAdapter(bigIntAdapter(), stringAdapter())));
  }

  @Test
  public void testMapColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("MAP<STRING,BIGINT>", MAP_PROPERTY_TEST_SEED,
        COMPLEX_PROPERTY_TEST_ITERATIONS, mapAdapter(stringAdapter(), bigIntAdapter()));
  }

  @Test
  public void testNestedMapColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("MAP<STRING,ARRAY<BIGINT>>", NESTED_MAP_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        mapAdapter(stringAdapter(), arrayAdapter(bigIntAdapter())));
  }

  @Test
  public void testMapContainingMapPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("MAP<STRING,MAP<BIGINT,STRING>>", MAP_MAP_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        mapAdapter(stringAdapter(), mapAdapter(bigIntAdapter(), stringAdapter())));
  }

  @Test
  public void testComplexMapKeyAndValueColumnSchema() throws Exception {
    assertRandomColumnRoundTrips("MAP<STRUCT<BIGINT,STRING>,UNIONTYPE<BIGINT,STRING>>",
        COMPLEX_MAP_PROPERTY_TEST_SEED, NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        mapAdapter(structAdapter(bigIntAdapter(), stringAdapter()),
            unionAdapter(bigIntAdapter(), stringAdapter())));
  }

  @Test
  public void testArrayContainingMapPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("ARRAY<MAP<STRING,BIGINT>>", ARRAY_MAP_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        arrayAdapter(mapAdapter(stringAdapter(), bigIntAdapter())));
  }

  @Test
  public void testStructContainingMapPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("STRUCT<MAP<STRING,BIGINT>,DECIMAL>",
        STRUCT_MAP_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        structAdapter(mapAdapter(stringAdapter(), bigIntAdapter()), decimalAdapter()));
  }

  @Test
  public void testUnionContainingMapPropertySchema() throws Exception {
    assertRandomColumnRoundTrips("UNIONTYPE<MAP<STRING,BIGINT>,STRING>",
        UNION_MAP_PROPERTY_TEST_SEED,
        NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS,
        unionAdapter(mapAdapter(stringAdapter(), bigIntAdapter()), stringAdapter()));
  }

  @Test
  public void testBytesColumnVectorSlices() throws Exception {
    assertRandomBytesSliceRoundTrips(BYTES_SLICE_PROPERTY_TEST_SEED,
        BYTES_SLICE_PROPERTY_TEST_ITERATIONS);
  }

  @Test
  public void testMultiColumnSourceMapping() throws Exception {
    assertRandomColumnMappingRoundTrips(COLUMN_MAPPING_PROPERTY_TEST_SEED,
        COLUMN_MAPPING_PROPERTY_TEST_ITERATIONS);
  }

  @Test
  public void testEmptySourceColumnMappingPreservesRows() throws Exception {
    VectorizedRowBatch source = batchWithColumn(new LongColumnVector(), 17);
    VectorizedRowBatch destination = new VectorizedRowBatch(0);
    roundTrip(source, new int[0], destination);
    assertEquals(17, destination.size);
    assertEquals(0, destination.projectionSize);
  }

  @Test
  public void testReinitializedSerdeAcceptsChangingSchemasAndMappings() throws Exception {
    VectorShuffleBatchSerializer dynamicSerializer = new VectorShuffleBatchSerializer();
    VectorShuffleBatchDeserializer dynamicDeserializer = new VectorShuffleBatchDeserializer();
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    DateColumnVector date = new DateColumnVector().setUsingProlepticCalendar(true);
    date.vector[0] = -200000;
    date.vector[1] = -199999;
    TimestampColumnVector timestamp = new TimestampColumnVector();
    timestamp.setIsUTC(true);
    timestamp.isRepeating = true;
    timestamp.time[0] = -20000000000000L;
    source.cols[0] = date;
    source.cols[1] = timestamp;
    source.size = 2;
    byte[] buffer = new byte[1024];
    for (int[] mapping : new int[][] {{0}, {1}, {1, 0, 1}, {}}) {
      VectorizedRowBatch destination = new VectorizedRowBatch(mapping.length);
      for (int column = 0; column < mapping.length; column++) {
        destination.cols[column] = mapping[column] == 0
            ? new DateColumnVector() : new TimestampColumnVector();
      }
      dynamicSerializer.initialize(source.cols, mapping);
      int length = dynamicSerializer.serialize(source, mapping, buffer, 0);
      dynamicDeserializer.deserialize(new BytesWritable(buffer, 0, length), destination);
      assertEquals(2, destination.size);
      assertEquals(mapping.length, destination.projectionSize);
      for (int column = 0; column < mapping.length; column++) {
        assertMetadataEquals(source.cols[mapping[column]], destination.cols[column]);
        if (mapping[column] == 0) {
          assertEquals(-199999, ((DateColumnVector) destination.cols[column]).vector[1]);
        } else {
          assertTrue(destination.cols[column].isRepeating);
          assertEquals(-20000000000000L,
              ((TimestampColumnVector) destination.cols[column]).time[0]);
        }
      }
    }
  }

  @Test
  public void testBatchSizeEncodingBoundaries() throws Exception {
    for (int size : new int[] {0, 1, 127, 128, 255, 256, 1023, 1024}) {
      LongColumnVector source = new LongColumnVector();
      for (int row = 0; row < size; row++) {
        source.vector[row] = row;
      }
      VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
      roundTrip(batchWithColumn(source, size), new int[] {0}, destination);
      assertEquals(size, destination.size);
      LongColumnVector actual = (LongColumnVector) destination.cols[0];
      for (int row = 0; row < size; row++) {
        assertEquals(row, actual.vector[row]);
      }
    }
  }

  @Test
  public void testExplicitRowIndicesSubset() throws Exception {
    LongColumnVector source = new LongColumnVector();
    for (int row = 0; row < 8; row++) {
      source.vector[row] = row * 10L;
    }
    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    BytesWritable serialized = new BytesWritable();

    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    VectorizedRowBatch batch = batchWithColumn(source, 8);
    int[] mapping = {0};
    serializer.initialize(batch.cols, mapping);
    int length = serializer.serialize(batch, mapping,
        new int[] {5, 1, 7}, 0, 3, buffer, 0);
    serialized.set(buffer, 0, length);
    deserializer.deserialize(serialized, destination);

    assertEquals(3, destination.size);
    LongColumnVector actual = (LongColumnVector) destination.cols[0];
    assertEquals(50L, actual.vector[0]);
    assertEquals(10L, actual.vector[1]);
    assertEquals(70L, actual.vector[2]);
  }


  @Test
  public void testCommonPrefixSetupAcrossPartitionsAndInputBatches() throws Exception {
    VectorShuffleBatchSerializer prefixSerializer = new VectorShuffleBatchSerializer();
    int[] mapping = {0, 1};
    int[] partitionRows = {0, 2, 4, 1, 3, 5};
    byte[] buffer = new byte[1024];
    byte[] expected = new byte[1024];
    int offset = 11;

    for (int batchNumber = 0; batchNumber < 2; batchNumber++) {
      VectorizedRowBatch source = new VectorizedRowBatch(2);
      LongColumnVector repeating = new LongColumnVector();
      repeating.isRepeating = true;
      repeating.vector[0] = 100 + batchNumber;
      LongColumnVector ordinary = new LongColumnVector();
      for (int row = 0; row < 6; row++) {
        ordinary.vector[row] = batchNumber * 1000 + row;
      }
      source.cols[0] = repeating;
      source.cols[1] = ordinary;
      source.size = 6;

      if (batchNumber == 0) {
        prefixSerializer.initialize(source.cols, mapping);
      }
      prefixSerializer.setupCommonPrefix(source, mapping, buffer, offset);
      for (int partition = 0; partition < 2; partition++) {
        int rowOffset = partition * 3;
        int expectedLength = prefixSerializer.serialize(
            source, mapping, partitionRows, rowOffset, 3, expected, offset);
        int actualLength = prefixSerializer.serializePartition(
            source, partitionRows, rowOffset, 3, buffer, offset);
        assertEquals(expectedLength, actualLength);
        assertArrayEquals(
            Arrays.copyOfRange(expected, offset, offset + expectedLength),
            Arrays.copyOfRange(buffer, offset, offset + actualLength));
      }
    }
  }

  @Test
  public void testSerializerCapacityReservationsForPrimitiveColumns() {
    LongColumnVector longs = new LongColumnVector();
    DoubleColumnVector doubles = new DoubleColumnVector();
    TimestampColumnVector timestamps = new TimestampColumnVector();
    IntervalDayTimeColumnVector intervals = new IntervalDayTimeColumnVector();
    BytesColumnVector strings = new BytesColumnVector();
    strings.initBuffer();

    for (int row = 0; row < 4; row++) {
      longs.vector[row] = row * 17L;
      doubles.vector[row] = row / 7.0;
      timestamps.time[row] = row * 1000L;
      timestamps.nanos[row] = row * 12345;
      intervals.set(row, new HiveIntervalDayTime(row * 3600L, row * 12345));
      strings.setVal(row, bytes("value-" + row));
    }
    longs.noNulls = false;
    longs.isNull[1] = true;
    doubles.noNulls = false;
    doubles.isNull[2] = true;
    timestamps.noNulls = false;
    timestamps.isNull[1] = true;
    intervals.noNulls = false;
    intervals.isNull[3] = true;
    strings.noNulls = false;
    strings.isNull[0] = true;

    for (ColumnVector column : new ColumnVector[] {
        longs, doubles, timestamps, intervals, strings}) {
      assertSerializeFitsExactBuffer(batchWithColumn(column, 4));
    }

    // A repeating nullable column writes one value, not four.
    longs.isRepeating = true;
    longs.isNull[0] = false;
    assertSerializeFitsExactBuffer(batchWithColumn(longs, 4));

    // Nullable-but-not-null and all-null columns reserve exactly their emitted payload.
    longs.isRepeating = false;
    Arrays.fill(longs.isNull, 0, 4, false);
    assertSerializeFitsExactBuffer(batchWithColumn(longs, 4));
    Arrays.fill(longs.isNull, 0, 4, true);
    assertSerializeFitsExactBuffer(batchWithColumn(longs, 4));
  }

  @Test
  public void testSerializerCapacityReservationsForNestedColumns() {
    LongColumnVector elements = new LongColumnVector();
    ListColumnVector list = new ListColumnVector(1024, elements);
    list.offsets[0] = 0;
    list.lengths[0] = 2;
    list.offsets[1] = 2;
    list.lengths[1] = 0;
    list.offsets[2] = 2;
    list.lengths[2] = 1;
    list.childCount = 3;
    elements.vector[0] = 10;
    elements.vector[1] = 20;
    elements.vector[2] = 30;
    list.noNulls = false;
    list.isNull[1] = true;
    assertSerializeFitsExactBuffer(batchWithColumn(list, 3));

    LongColumnVector keys = new LongColumnVector();
    BytesColumnVector values = new BytesColumnVector();
    values.initBuffer();
    MapColumnVector map = new MapColumnVector(1024, keys, values);
    map.offsets[0] = 0;
    map.lengths[0] = 1;
    map.offsets[1] = 1;
    map.lengths[1] = 0;
    map.offsets[2] = 1;
    map.lengths[2] = 1;
    map.childCount = 2;
    keys.vector[0] = 7;
    keys.vector[1] = 9;
    values.setVal(0, bytes("one"));
    values.setVal(1, bytes("two"));
    assertSerializeFitsExactBuffer(batchWithColumn(map, 3));

    LongColumnVector unionLongs = new LongColumnVector();
    TimestampColumnVector unionTimestamps = new TimestampColumnVector();
    UnionColumnVector union = unionOf(unionLongs, unionTimestamps);
    union.tags[0] = 0;
    union.tags[1] = 1;
    union.tags[2] = 0;
    unionLongs.vector[0] = 5;
    unionLongs.vector[2] = 8;
    unionTimestamps.time[1] = 123456L;
    unionTimestamps.nanos[1] = 789;
    assertSerializeFitsExactBuffer(batchWithColumn(union, 3));
  }

  private static void assertSerializeFitsExactBuffer(VectorizedRowBatch source) {
    VectorShuffleBatchSerializer batchSerializer = new VectorShuffleBatchSerializer();
    int[] mapping = identityMap(source.numCols);
    batchSerializer.initialize(source.cols, mapping);
    int offset = 13;
    byte[] reference = new byte[4096];
    int length = batchSerializer.serialize(source, mapping, reference, offset);

    byte[] exact = new byte[offset + length];
    assertEquals(length, batchSerializer.serialize(source, mapping, exact, offset));
    assertArrayEquals(
        Arrays.copyOfRange(reference, offset, offset + length),
        Arrays.copyOfRange(exact, offset, offset + length));

    byte[] tooSmall = new byte[offset + length - 1];
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> batchSerializer.serialize(source, mapping, tooSmall, offset));
  }

  @Test
  public void testDenseFixedWidthFastPathsForConsecutiveSelectedAndPartitionRows()
      throws Exception {
    for (int rows : new int[] {1, 7, 8, 10, 16}) {
      for (int mode = 0; mode < 3; mode++) {
        LongColumnVector longs = new LongColumnVector();
        DoubleColumnVector doubles = new DoubleColumnVector();
        for (int physical = 0; physical < 64; physical++) {
          longs.vector[physical] = physical * 17L - 2026L;
          doubles.vector[physical] = physical % 4 == 0
              ? Double.longBitsToDouble(0x7ff80000000000f1L)
              : physical % 3 == 1 ? -0.0d : physical / 7.0d;
        }
        VectorizedRowBatch source = new VectorizedRowBatch(2);
        source.cols[0] = longs;
        source.cols[1] = doubles;
        source.size = rows;
        int[] selected = new int[rows + 1];
        selected[0] = 99; // Must not be read for partition subsets with offset 1.
        for (int i = 0; i < rows; i++) {
          source.selected[i] = (i * 3 + 1) % 64;
          selected[i + 1] = (i * 7 + 3) % 64;
        }
        source.selectedInUse = mode == 1;
        int[] mapping = {0, 1};
        serializer.initialize(source.cols, mapping);
        byte[] bytes = new byte[2048];
        int frameOffset = 11;
        int length = mode == 2
            ? serializer.serialize(source, mapping, selected, 1, rows, bytes, frameOffset)
            : serializer.serialize(source, mapping, bytes, frameOffset);
        // Bitmap, nested flag, and one column flag per column; unchanged wire format.
        assertEquals(10 + 2 * (1 + rows * Long.BYTES), length);
        BytesWritable serialized = new BytesWritable(bytes, frameOffset, length);
        for (boolean accumulate : new boolean[] {false, true}) {
          VectorizedRowBatch destination = new VectorizedRowBatch(2);
          destination.cols[0] = new LongColumnVector();
          destination.cols[1] = new DoubleColumnVector();
          destination.reset();
          if (accumulate) {
            VectorShuffleBatchAccumulatingDeserializer decoder =
                new VectorShuffleBatchAccumulatingDeserializer(destination, 2);
            decoder.readHeader(serialized);
            decoder.append(serialized, destination);
          } else {
            new VectorShuffleBatchDeserializer(destination, 2)
                .deserialize(serialized, destination);
          }
          assertEquals(rows, destination.size);
          for (int i = 0; i < rows; i++) {
            int physical = mode == 0 ? i
                : mode == 1 ? source.selected[i] : selected[i + 1];
            assertFalse(destination.cols[0].isNull[i]);
            assertFalse(destination.cols[1].isNull[i]);
            assertEquals(longs.vector[physical],
                ((LongColumnVector) destination.cols[0]).vector[i]);
            assertEquals(Double.doubleToLongBits(doubles.vector[physical]),
                Double.doubleToLongBits(((DoubleColumnVector) destination.cols[1]).vector[i]));
          }
        }
      }
    }
  }

  @Test
  public void testDenseAppendAfterNullableFrameClearsNullBits() throws Exception {
    LongColumnVector nullable = new LongColumnVector();
    nullable.noNulls = false;
    nullable.isNull[1] = true;
    nullable.vector[0] = 10;
    nullable.vector[2] = 30;
    VectorizedRowBatch first = batchWithColumn(nullable, 3);

    LongColumnVector dense = new LongColumnVector();
    for (int i = 0; i < 10; i++) {
      dense.vector[i] = 100 + i;
    }
    VectorizedRowBatch next = batchWithColumn(dense, 10);
    int[] mapping = {0};
    byte[] bytes = new byte[2048];
    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    serializer.initialize(first.cols, mapping);

    for (VectorizedRowBatch source : new VectorizedRowBatch[] {first, next}) {
      int length = serializer.serialize(source, mapping, bytes, 13);
      BytesWritable frame = new BytesWritable(bytes, 13, length);
      decoder.readHeader(frame);
      if (destination.size > 0) {
        assertTrue(decoder.isCompatible());
      }
      decoder.append(frame, destination);
    }

    assertEquals(13, destination.size);
    assertFalse(destination.cols[0].noNulls);
    assertTrue(destination.cols[0].isNull[1]);
    for (int i = 3; i < 13; i++) {
      assertFalse(destination.cols[0].isNull[i]);
      assertEquals(100 + i - 3, ((LongColumnVector) destination.cols[0]).vector[i]);
    }
    // Reusing a standalone destination must not expose stale isNull bits.
    int length = serializer.serialize(next, mapping, bytes, 13);
    new VectorShuffleBatchDeserializer(destination, 1)
        .deserialize(new BytesWritable(bytes, 13, length), destination);
    assertEquals(10, destination.size);
    for (int i = 0; i < 10; i++) {
      assertFalse(destination.cols[0].isNull[i]);
    }
  }

  @Test
  public void testNullBitmapBoundaries() throws Exception {
    LongColumnVector source = new LongColumnVector();
    source.noNulls = false;
    for (int row = 0; row < 17; row++) {
      source.vector[row] = row * 10L;
    }
    for (int row : new int[] {0, 7, 8, 15, 16}) {
      source.isNull[row] = true;
    }

    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    roundTrip(batchWithColumn(source, 17), new int[] {0}, destination);
    LongColumnVector actual = (LongColumnVector) destination.cols[0];
    for (int row = 0; row < 17; row++) {
      assertEquals(source.isNull[row], actual.isNull[row]);
      if (!source.isNull[row]) {
        assertEquals(source.vector[row], actual.vector[row]);
      }
    }
  }

  @Test
  public void testNullBitmapReuseWithConsecutiveSelectedAndPartitionRows() throws Exception {
    LongColumnVector longs = new LongColumnVector();
    BytesColumnVector strings = new BytesColumnVector();
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    source.cols[0] = longs;
    source.cols[1] = strings;
    source.reset();
    source.size = 17;
    longs.noNulls = false;
    strings.noNulls = false;
    for (int row = 0; row < 40; row++) {
      longs.vector[row] = row * 10L;
      strings.setVal(row, bytes("row-" + row));
    }
    int[] mapping = {0, 1};
    serializer.initialize(source.cols, mapping);
    byte[] buffer = new byte[2048];
    int offset = 11;
    int[] subset = {99, 31, 7, 29, 1, 16, 8, 0, 23, 15, 30, 4, 25, 6, 28, 2, 19, 9, 99};
    for (int mode = 0; mode < 3; mode++) {
      source.selectedInUse = mode == 1;
      System.arraycopy(subset, 1, source.selected, 0, source.size);
      // Reuse a dirty buffer across all-null, sparse-null, and nullable-but-non-null frames.
      for (int nullPattern = 0; nullPattern < 3; nullPattern++) {
        Arrays.fill(buffer, (byte) 0xff);
        for (int row = 0; row < 40; row++) {
          longs.isNull[row] = nullPattern == 0 || (nullPattern == 1 && row % 3 == 0);
          strings.isNull[row] = nullPattern == 0 || (nullPattern == 1 && row % 5 == 0);
        }
        int length = mode == 2
            ? serializer.serialize(source, mapping, subset, 1, source.size, buffer, offset)
            : serializer.serialize(source, mapping, buffer, offset);
        assertEquals(0xff, buffer[offset - 1] & 0xff);
        assertEquals(0xff, buffer[offset + length] & 0xff);
        BytesWritable serialized = new BytesWritable(buffer, offset, length);
        for (boolean accumulate : new boolean[] {false, true}) {
          VectorizedRowBatch destination = new VectorizedRowBatch(2);
          destination.cols[0] = new LongColumnVector();
          destination.cols[1] = new BytesColumnVector();
          destination.reset();
          if (accumulate) {
            VectorShuffleBatchAccumulatingDeserializer decoder =
                new VectorShuffleBatchAccumulatingDeserializer(destination, 2);
            decoder.readHeader(serialized);
            decoder.append(serialized, destination);
          } else {
            deserializer.deserialize(serialized, destination);
          }
          assertEquals(source.size, destination.size);
          for (int row = 0; row < source.size; row++) {
            int physical = mode == 0 ? row : subset[row + 1];
            assertEquals(longs.isNull[physical], destination.cols[0].isNull[row]);
            assertEquals(strings.isNull[physical], destination.cols[1].isNull[row]);
            if (!longs.isNull[physical]) {
              assertEquals(longs.vector[physical],
                  ((LongColumnVector) destination.cols[0]).vector[row]);
            }
            if (!strings.isNull[physical]) {
              assertBytesValueEquals(bytes("row-" + physical),
                  (BytesColumnVector) destination.cols[1], row);
            }
          }
        }
      }
    }
  }

  @Test
  public void testSerializerRejectsNegativeBytesColumnLength() {
    BytesColumnVector bytes = new BytesColumnVector();
    VectorizedRowBatch source = batchWithColumn(bytes, 1);
    VectorShuffleBatchSerializer batchSerializer = new VectorShuffleBatchSerializer();
    int[] mapping = {0};
    batchSerializer.initialize(source.cols, mapping);
    byte[] buffer = new byte[1024];

    for (int length : new int[] {-1, -4, Integer.MIN_VALUE}) {
      bytes.setRef(0, new byte[0], 0, length);
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
          () -> batchSerializer.serialize(source, mapping, buffer, 0));
      assertEquals("Negative byte length: " + length, exception.getMessage());
    }
  }

  @Test
  public void testVariableLengthEncodingBoundaries() throws Exception {
    int[] lengths = {0, 1, 127, 128, 255, 256};
    BytesColumnVector source = new BytesColumnVector();
    source.initBuffer();
    for (int row = 0; row < lengths.length; row++) {
      byte[] value = new byte[lengths[row]];
      Arrays.fill(value, (byte) ('a' + row));
      source.setVal(row, value);
    }

    VectorizedRowBatch destination = batchWithColumn(stringAdapter().createVector(), 0);
    roundTrip(batchWithColumn(source, lengths.length), new int[] {0}, destination);
    BytesColumnVector actual = (BytesColumnVector) destination.cols[0];
    for (int row = 0; row < lengths.length; row++) {
      assertEquals(lengths[row], actual.length[row]);
      byte[] expected = new byte[lengths[row]];
      Arrays.fill(expected, (byte) ('a' + row));
      assertBytesValueEquals(expected, actual, row);
    }
  }

  @Test
  public void testMultiValuedLengthEncodingBoundaries() throws Exception {
    int[] lengths = {0, 1, 127, 128, 255, 256};
    LongColumnVector child = new LongColumnVector(1024);
    ListColumnVector source = new ListColumnVector(lengths.length, child);
    int childIndex = 0;
    for (int row = 0; row < lengths.length; row++) {
      source.offsets[row] = childIndex;
      source.lengths[row] = lengths[row];
      for (int element = 0; element < lengths[row]; element++) {
        child.vector[childIndex++] = ((long) row << 32) | element;
      }
    }
    source.childCount = childIndex;

    ListColumnVector actual = new ListColumnVector(lengths.length, new LongColumnVector());
    VectorizedRowBatch destination = batchWithColumn(actual, 0);
    roundTrip(batchWithColumn(source, lengths.length), new int[] {0}, destination);
    LongColumnVector actualChild = (LongColumnVector) actual.child;
    assertEquals(source.childCount, actual.childCount);
    for (int row = 0; row < lengths.length; row++) {
      assertEquals(lengths[row], actual.lengths[row]);
      for (int element = 0; element < lengths[row]; element++) {
        assertEquals(((long) row << 32) | element,
            actualChild.vector[(int) actual.offsets[row] + element]);
      }
    }
  }

  @Test
  public void testAccumulationPreservesRepeatingAndFlushesOnConflict() throws Exception {
    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);

    LongColumnVector repeated = new LongColumnVector();
    repeated.isRepeating = true;
    repeated.vector[0] = 17;
    BytesWritable first = serialize(batchWithColumn(repeated, 3));
    assertFalse(accumulating.readHeader(first).hasNestedRepeating());
    accumulating.append(first, destination);
    assertEquals(3, destination.size);
    assertTrue(destination.cols[0].isRepeating);
    assertTrue(accumulating.hasRepeatingColumns());

    BytesWritable identical = serialize(batchWithColumn(repeated, 4));
    assertTrue(isCompatible(accumulating, identical));
    accumulating.append(identical, destination);
    assertEquals(7, destination.size);
    assertTrue(destination.cols[0].isRepeating);
    assertEquals(17, ((LongColumnVector) destination.cols[0]).vector[0]);

    // One-row frames have no effective top-level repetition and are dispatched standalone.
    LongColumnVector single = new LongColumnVector();
    single.vector[0] = 17;
    BytesWritable one = serialize(batchWithColumn(single, 1));
    assertFalse(isCompatible(accumulating, one));
    VectorizedRowBatch oneDestination = batchWithColumn(new LongColumnVector(), 0);
    accumulating.deserialize(one, oneDestination);
    assertEquals(1, oneDestination.size);
    assertEquals(17, ((LongColumnVector) oneDestination.cols[0]).vector[0]);
    assertEquals(7, destination.size);
    assertTrue(destination.cols[0].isRepeating);

    LongColumnVector changed = new LongColumnVector();
    changed.isRepeating = true;
    changed.vector[0] = 23;
    BytesWritable different = serialize(batchWithColumn(changed, 2));
    assertFalse(isCompatible(accumulating, different));

    // Simulate the receiver flushing the old batch before appending the conflicting frame.
    destination.reset();
    accumulating.resetAccumulation();
    accumulating.append(different, destination);
    assertEquals(2, destination.size);
    assertTrue(destination.cols[0].isRepeating);
    assertEquals(23, ((LongColumnVector) destination.cols[0]).vector[0]);

    LongColumnVector dense = new LongColumnVector();
    dense.vector[0] = 31;
    dense.vector[1] = 37;
    BytesWritable nonRepeating = serialize(batchWithColumn(dense, 2));
    assertFalse(isCompatible(accumulating, nonRepeating));

    destination.reset();
    accumulating.resetAccumulation();
    accumulating.append(nonRepeating, destination);
    assertEquals(2, destination.size);
    assertFalse(destination.cols[0].isRepeating);
    assertEquals(31, ((LongColumnVector) destination.cols[0]).vector[0]);
    assertEquals(37, ((LongColumnVector) destination.cols[0]).vector[1]);

    assertFalse(isCompatible(accumulating, first));
  }

  @Test
  public void testAccumulationMixedRepeatingAndDenseColumns() throws Exception {
    VectorizedRowBatch destination = new VectorizedRowBatch(2);
    destination.cols[0] = new LongColumnVector();
    destination.cols[1] = new LongColumnVector();
    destination.projectionSize = 2;
    destination.reset();
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 2);

    VectorizedRowBatch first = new VectorizedRowBatch(2);
    LongColumnVector firstRepeating = new LongColumnVector();
    firstRepeating.isRepeating = true;
    firstRepeating.vector[0] = 7;
    LongColumnVector firstDense = new LongColumnVector();
    firstDense.vector[0] = 11;
    firstDense.vector[1] = 13;
    first.cols[0] = firstRepeating;
    first.cols[1] = firstDense;
    first.size = 2;

    VectorizedRowBatch second = new VectorizedRowBatch(2);
    LongColumnVector secondRepeating = new LongColumnVector();
    secondRepeating.isRepeating = true;
    secondRepeating.vector[0] = 7;
    LongColumnVector secondDense = new LongColumnVector();
    secondDense.vector[0] = 17;
    secondDense.vector[1] = 19;
    secondDense.vector[2] = 23;
    second.cols[0] = secondRepeating;
    second.cols[1] = secondDense;
    second.size = 3;

    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    int[] mapping = {0, 1};
    serializer.initialize(first.cols, mapping);
    int length = serializer.serialize(first, mapping, buffer, 0);
    BytesWritable firstEncoded = new BytesWritable();
    firstEncoded.set(buffer, 0, length);
    accumulating.readHeader(firstEncoded);
    accumulating.append(firstEncoded, destination);

    length = serializer.serialize(second, mapping, buffer, 0);
    BytesWritable secondEncoded = new BytesWritable();
    secondEncoded.set(buffer, 0, length);
    assertTrue(isCompatible(accumulating, secondEncoded));
    accumulating.append(secondEncoded, destination);

    assertEquals(5, destination.size);
    assertTrue(destination.cols[0].isRepeating);
    assertFalse(destination.cols[1].isRepeating);
    assertEquals(7, ((LongColumnVector) destination.cols[0]).vector[0]);
    LongColumnVector actualDense = (LongColumnVector) destination.cols[1];
    assertEquals(11, actualDense.vector[0]);
    assertEquals(13, actualDense.vector[1]);
    assertEquals(17, actualDense.vector[2]);
    assertEquals(19, actualDense.vector[3]);
    assertEquals(23, actualDense.vector[4]);

    secondRepeating.vector[0] = 8;
    length = serializer.serialize(second, mapping, buffer, 0);
    BytesWritable incompatible = new BytesWritable();
    incompatible.set(buffer, 0, length);
    assertFalse(isCompatible(accumulating, incompatible));
  }

  @Test
  public void testAccumulationByteColumnBoundaries() throws Exception {
    byte[][] values = {bytes("first"), bytes(""), null};
    VectorizedRowBatch source = new VectorizedRowBatch(2);
    BytesColumnVector strings = new BytesColumnVector();
    LongColumnVector longs = new LongColumnVector();
    source.cols[0] = strings;
    source.cols[1] = longs;
    source.reset();
    source.size = values.length;
    for (int row = 0; row < values.length; row++) {
      if (values[row] == null) {
        strings.noNulls = false;
        strings.isNull[row] = true;
      } else {
        strings.setVal(row, values[row]);
      }
    }

    VectorizedRowBatch destination = new VectorizedRowBatch(2);
    destination.cols[0] = new BytesColumnVector();
    destination.cols[1] = new LongColumnVector();
    destination.reset();
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 2);
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    int offset = 13;
    int[] mapping = {0, 1};
    serializer.initialize(source.cols, mapping);
    for (int frame = 0; frame < 2; frame++) {
      for (int row = 0; row < values.length; row++) {
        longs.vector[row] = frame * 10 + row;
      }
      int length = serializer.serialize(source, mapping, buffer, offset);
      BytesWritable serialized = new BytesWritable(buffer, offset, length);
      assertFalse(accumulating.readHeader(serialized).hasNestedRepeating());
      if (destination.size > 0) {
        assertTrue(accumulating.isCompatible());
      }
      accumulating.append(serialized, destination);
      // The reader may reuse its backing buffer as soon as append returns.
      Arrays.fill(buffer, (byte) 0xff);

      assertEquals((frame + 1) * values.length, destination.size);
      BytesColumnVector actualStrings = (BytesColumnVector) destination.cols[0];
      LongColumnVector actualLongs = (LongColumnVector) destination.cols[1];
      assertFalse(actualStrings.noNulls);
      for (int row = 0; row < destination.size; row++) {
        int sourceRow = row % values.length;
        assertEquals(values[sourceRow] == null, actualStrings.isNull[row]);
        if (values[sourceRow] != null) {
          assertBytesValueEquals(values[sourceRow], actualStrings, row);
        }
        assertEquals((row / values.length) * 10 + sourceRow, actualLongs.vector[row]);
      }
    }
  }

  @Test
  public void testAccumulationRepeatingListOfStrings() throws Exception {
    BytesColumnVector child = new BytesColumnVector();
    child.initBuffer();
    child.setVal(0, bytes("x"));
    child.setVal(1, bytes(""));
    child.noNulls = false;
    child.isNull[2] = true;
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
    list.isRepeating = true;
    list.lengths[0] = 3;
    list.childCount = 3;
    VectorizedRowBatch source = batchWithColumn(list, 3);

    VectorizedRowBatch destination = batchWithColumn(
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new BytesColumnVector()), 0);
    destination.reset();
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    BytesWritable serialized = serialize(source);
    for (int frame = 0; frame < 2; frame++) {
      assertFalse(accumulating.readHeader(serialized).hasNestedRepeating());
      if (destination.size > 0) {
        assertTrue(accumulating.isCompatible());
      }
      accumulating.append(serialized, destination);
      assertEquals((frame + 1) * source.size, destination.size);
      ListColumnVector actual = (ListColumnVector) destination.cols[0];
      assertTrue(actual.isRepeating);
      assertEquals(0, actual.offsets[0]);
      assertEquals(3, actual.lengths[0]);
      assertEquals(3, actual.childCount);
      BytesColumnVector actualChild = (BytesColumnVector) actual.child;
      assertBytesValueEquals(bytes("x"), actualChild, 0);
      assertBytesValueEquals(bytes(""), actualChild, 1);
      assertFalse(actualChild.noNulls);
      assertTrue(actualChild.isNull[2]);
    }

    child.setVal(0, bytes("different"));
    assertFalse(isCompatible(accumulating, serialize(source)));
  }

  @Test
  public void testNestedRepeatingFrameUsesStandaloneDecoding() throws Exception {
    LongColumnVector child = new LongColumnVector();
    child.isRepeating = true;
    child.vector[0] = 41;
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
    list.offsets[0] = 0;
    list.lengths[0] = 3;
    list.childCount = 3;

    BytesWritable serialized = serialize(batchWithColumn(list, 1));
    VectorizedRowBatch destination = batchWithColumn(
        new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector()), 0);
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);

    assertTrue(accumulating.readHeader(serialized).hasNestedRepeating());

    // The receiver dispatches such a frame alone; it never expands the repeated children.
    accumulating.deserialize(serialized, destination);
    ListColumnVector decoded = (ListColumnVector) destination.cols[0];
    assertEquals(1, destination.size);
    assertEquals(3, decoded.lengths[0]);
    assertTrue(decoded.child.isRepeating);
    assertEquals(41, ((LongColumnVector) decoded.child).vector[0]);
  }

  @Test
  public void testAccumulatingDecoderRejectsTrailingBytes() throws Exception {
    LongColumnVector source = new LongColumnVector();
    source.vector[0] = 7;
    source.vector[1] = 8;
    BytesWritable serialized = serialize(batchWithColumn(source, 2));
    byte[] bytes = Arrays.copyOf(serialized.getBytes(), serialized.getLength() + 1);
    BytesWritable trailing = new BytesWritable(bytes);

    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    accumulating.readHeader(trailing);
    assertThrows(IOException.class, () -> accumulating.append(trailing, destination));
    assertThrows(IOException.class,
        () -> accumulating.deserialize(trailing, destination));
  }

  @Test
  public void testRepeatingBitmapUsesLogicalOrderAndPartitionOffsets() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(12);
    source.size = 3;
    source.selectedInUse = true;
    source.selected[0] = 7;
    source.selected[1] = 1;
    source.selected[2] = 5;
    for (int column = 0; column < source.cols.length; column++) {
      LongColumnVector values = new LongColumnVector();
      for (int row = 0; row < 8; row++) {
        values.vector[row] = column * 100L + row;
      }
      source.cols[column] = values;
    }
    for (int column : new int[] {9, 3, 0, 10}) {
      source.cols[column].isRepeating = true;
    }
    source.cols[10].noNulls = false;
    source.cols[10].isNull[0] = true;
    // Equal dense values must not be promoted into the repeating section.
    Arrays.fill(((LongColumnVector) source.cols[2]).vector, 42L);
    int[] mapping = {9, 2, 8, 1, 6, 4, 11, 3, 0, 10};
    serializer.initialize(source.cols, mapping);
    byte[] wholeBuffer = new byte[1024];
    byte[] partitionBuffer = new byte[1024];
    Arrays.fill(wholeBuffer, (byte) 0x7f);
    Arrays.fill(partitionBuffer, (byte) 0x7f);
    int offset = 11;
    int length = serializer.serialize(source, mapping, wholeBuffer, offset);
    int partitionLength = serializer.serialize(source, mapping, new int[] {99, 7, 1, 5, 99},
        1, 3, partitionBuffer, offset);
    assertEquals(length, partitionLength);
    assertArrayEquals(wholeBuffer, partitionBuffer);
    assertEquals(0x7f, wholeBuffer[offset - 1]);
    assertEquals(0x7f, wholeBuffer[offset + length]);

    BytesWritable serialized = new BytesWritable(wholeBuffer, offset, length);
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialized, mapping.length, 0);
    assertEquals(2, header.getBitmapLength());
    assertEquals(0x81, wholeBuffer[header.getBitmapOffset()] & 0xff);
    assertEquals(0x03, wholeBuffer[header.getBitmapOffset() + 1] & 0xff);
    assertEquals(29, header.getRepeatingEnd() - header.getRepeatingStart());
    assertEquals(900L, ByteBuffer.wrap(wholeBuffer).order(ByteOrder.nativeOrder())
        .getLong(header.getRepeatingStart() + 1));
    assertEquals(300L, ByteBuffer.wrap(wholeBuffer).order(ByteOrder.nativeOrder())
        .getLong(header.getRepeatingStart() + 10));
    assertFalse(header.hasNestedRepeating());
    for (boolean accumulate : new boolean[] {false, true}) {
      VectorizedRowBatch destination = new VectorizedRowBatch(mapping.length);
      for (int column = 0; column < mapping.length; column++) {
        destination.cols[column] = new LongColumnVector();
      }
      destination.reset();
      if (accumulate) {
        VectorShuffleBatchAccumulatingDeserializer decoder =
            new VectorShuffleBatchAccumulatingDeserializer(destination, mapping.length);
        decoder.readHeader(serialized);
        decoder.append(serialized, destination);
      } else {
        deserializer.deserialize(serialized, destination);
      }
      assertEquals(source.size, destination.size);
      for (int column = 0; column < mapping.length; column++) {
        ColumnVector original = source.cols[mapping[column]];
        LongColumnVector actual = (LongColumnVector) destination.cols[column];
        assertEquals(original.isRepeating, actual.isRepeating);
        for (int row = 0; row < source.size; row++) {
          int actualIndex = actual.isRepeating ? 0 : row;
          assertEquals(mapping[column] == 10, isNull(actual, actualIndex));
          if (mapping[column] != 10) {
            int sourceIndex = original.isRepeating ? 0 : source.selected[row];
            assertEquals(((LongColumnVector) original).vector[sourceIndex],
                actual.vector[actualIndex]);
          }
        }
      }
    }
  }

  @Test
  public void testRepeatingBitmapWordAndArrayBoundaries() throws Exception {
    for (int columns : new int[] {0, 1, 2, 63, 64, 65, 73}) {
      for (boolean repeating : new boolean[] {false, true}) {
        VectorizedRowBatch source = new VectorizedRowBatch(columns);
        VectorizedRowBatch destination = new VectorizedRowBatch(columns);
        for (int column = 0; column < columns; column++) {
          LongColumnVector values = new LongColumnVector();
          values.vector[0] = column * 10L;
          values.vector[1] = column * 10L + 1;
          values.isRepeating = repeating && (column == 0 || column == columns - 1);
          source.cols[column] = values;
          destination.cols[column] = new LongColumnVector();
        }
        source.size = 2;
        destination.reset();
        int[] mapping = identityMap(columns);
        serializer.initialize(source.cols, mapping);
        byte[] buffer = new byte[4096];
        Arrays.fill(buffer, (byte) 0xff);
        int length = serializer.serialize(source, mapping, buffer, 7);
        BytesWritable serialized = new BytesWritable(buffer, 7, length);
        VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
        header.read(serialized, columns, 0);
        assertEquals(repeating && columns != 0, header.hasRepeatingColumns());
        for (int column = 0; column < columns; column++) {
          assertEquals(source.cols[column].isRepeating, header.isRepeating(column));
        }
        if (columns <= Long.SIZE) {
          long expected = repeating && columns != 0 ? 1L | (1L << (columns - 1)) : 0;
          assertEquals(expected, header.getRepeatingBits());
        }
        VectorShuffleBatchDeserializer legacy =
            new VectorShuffleBatchDeserializer(destination, columns);
        legacy.deserialize(serialized, destination);
        assertEquals(2, destination.size);
        for (int column = 0; column < columns; column++) {
          assertEquals(source.cols[column].isRepeating, destination.cols[column].isRepeating);
        }
        destination.reset();
        VectorShuffleBatchAccumulatingDeserializer decoder =
            new VectorShuffleBatchAccumulatingDeserializer(destination, columns);
        decoder.readHeader(serialized);
        decoder.append(serialized, destination);
        decoder.readHeader(serialized);
        assertTrue(decoder.isCompatible());
        decoder.append(serialized, destination);
        assertEquals(4, destination.size);
        for (int column = 0; column < columns; column++) {
          LongColumnVector actual = (LongColumnVector) destination.cols[column];
          assertEquals(source.cols[column].isRepeating, actual.isRepeating);
          assertEquals(column * 10L, actual.vector[0]);
          if (!actual.isRepeating) {
            assertEquals(column * 10L + 1, actual.vector[3]);
          }
        }
      }
    }
  }

  @Test
  public void testCompatibilityReadsNoOrdinaryPayload() throws Exception {
    for (boolean repeating : new boolean[] {false, true}) {
      VectorizedRowBatch source = new VectorizedRowBatch(2);
      LongColumnVector representative = new LongColumnVector();
      representative.isRepeating = repeating;
      representative.vector[0] = 7;
      representative.vector[1] = 9;
      source.cols[0] = representative;
      source.cols[1] = new BytesColumnVector();
      ((BytesColumnVector) source.cols[1]).initBuffer();
      ((BytesColumnVector) source.cols[1]).setVal(0, bytes("first"));
      ((BytesColumnVector) source.cols[1]).setVal(1, bytes("second"));
      source.size = 2;
      byte[] buffer = new byte[1024];
      int[] mapping = {0, 1};
      serializer.initialize(source.cols, mapping);
      int length = serializer.serialize(source, mapping, buffer, 0);
      BytesWritable serialized = new BytesWritable(buffer, 0, length);
      VectorizedRowBatch destination = new VectorizedRowBatch(2);
      destination.cols[0] = new LongColumnVector();
      destination.cols[1] = new BytesColumnVector();
      destination.reset();
      VectorShuffleBatchAccumulatingDeserializer decoder =
          new VectorShuffleBatchAccumulatingDeserializer(destination, 2);
      decoder.readHeader(serialized);
      decoder.append(serialized, destination);

      VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
      header.read(serialized, 2, 0);
      // Removing every ordinary payload byte must not affect header/repeating compatibility.
      BytesWritable headerAndRepeating = new BytesWritable(buffer, 0, header.getOrdinaryStart());
      decoder.readHeader(headerAndRepeating);
      assertTrue(decoder.isCompatible());
      assertEquals(2, destination.size);
      assertEquals(7, ((LongColumnVector) destination.cols[0]).vector[0]);
      assertBytesValueEquals(bytes("second"), (BytesColumnVector) destination.cols[1], 1);
      decoder.releaseHeader();
    }
  }

  @Test
  public void testRepeatingSectionCacheOwnsBorrowedBytes() throws Exception {
    BytesColumnVector repeated = new BytesColumnVector();
    repeated.initBuffer();
    repeated.isRepeating = true;
    repeated.setVal(0, bytes("alpha"));
    VectorizedRowBatch source = batchWithColumn(repeated, 2);
    VectorizedRowBatch destination = batchWithColumn(new BytesColumnVector(), 0);
    destination.reset();
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    byte[] buffer = new byte[1024];
    int offset = 13;
    int[] mapping = {0};
    serializer.initialize(source.cols, mapping);
    int length = serializer.serialize(source, mapping, buffer, offset);
    BytesWritable reused = new BytesWritable(buffer, offset, length);
    decoder.readHeader(reused);
    decoder.append(reused, destination);
    Arrays.fill(buffer, (byte) 0xff);
    assertBytesValueEquals(bytes("alpha"), (BytesColumnVector) destination.cols[0], 0);

    repeated.setVal(0, bytes("alpHa"));
    length = serializer.serialize(source, new int[] {0}, buffer, offset);
    reused.setDirect(buffer, offset, length);
    assertFalse(isCompatible(decoder, reused));
    assertEquals(2, destination.size);
    assertBytesValueEquals(bytes("alpha"), (BytesColumnVector) destination.cols[0], 0);

    repeated.setVal(0, bytes("alpha"));
    length = serializer.serialize(source, new int[] {0}, buffer, offset);
    reused.setDirect(buffer, offset, length);
    assertTrue(isCompatible(decoder, reused));
    decoder.append(reused, destination);
    assertEquals(4, destination.size);
    assertTrue(destination.cols[0].isRepeating);
    assertBytesValueEquals(bytes("alpha"), (BytesColumnVector) destination.cols[0], 0);

    // Length is part of exact comparison, including when the decoded value has a common prefix.
    repeated.setVal(0, bytes("alpha!"));
    length = serializer.serialize(source, new int[] {0}, buffer, offset);
    reused.setDirect(buffer, offset, length);
    assertFalse(isCompatible(decoder, reused));
    decoder.releaseHeader();
  }

  @Test
  public void testCanonicalMetadataIncludesEmptyAndInactiveChildren() throws Exception {
    VectorizedRowBatch source = metadataBatch();
    BytesWritable serialized = serializeColumns(source);
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialized, 4, 2);
    assertEquals(0x02, serialized.getBytesRaw()[header.getBitmapOffset()] & 0xff);
    assertEquals(0xe3, serialized.getBytesRaw()[header.getMetadataOffset()] & 0xff);
    assertEquals(0x01, serialized.getBytesRaw()[header.getMetadataOffset() + 1] & 0xff);
    for (boolean accumulate : new boolean[] {false, true}) {
      VectorizedRowBatch destination = metadataBatch();
      ((TimestampColumnVector) destination.cols[1]).setIsUTC(false);
      ((TimestampColumnVector) destination.cols[1]).setUsingProlepticCalendar(true);
      ((DateColumnVector) ((ListColumnVector) destination.cols[0]).child)
          .setUsingProlepticCalendar(false);
      if (accumulate) {
        VectorShuffleBatchAccumulatingDeserializer decoder =
            new VectorShuffleBatchAccumulatingDeserializer(destination, 4);
        destination.reset();
        decoder.readHeader(serialized);
        decoder.append(serialized, destination);
        assertTrue(isCompatible(decoder, serialized));
        decoder.append(serialized, destination);
        assertEquals(4, destination.size);
        // The unused map key still contributes metadata and must force a flush on mismatch.
        ((DateColumnVector) ((MapColumnVector) source.cols[2]).keys)
            .setUsingProlepticCalendar(true);
        assertFalse(isCompatible(decoder, serializeColumns(source)));
        ((DateColumnVector) ((MapColumnVector) source.cols[2]).keys)
            .setUsingProlepticCalendar(false);
        assertEquals(4, destination.size);
        decoder.releaseHeader();
      } else {
        deserializer.deserialize(serialized, destination);
      }
      assertTrue(((DateColumnVector) ((ListColumnVector) destination.cols[0]).child)
          .isUsingProlepticCalendar());
      TimestampColumnVector timestamp = (TimestampColumnVector) destination.cols[1];
      assertTrue(timestamp.isUTC());
      assertFalse(timestamp.usingProlepticCalendar());
      assertEquals(-20000000000000L, timestamp.time[0]);
      assertEquals(123456789, timestamp.nanos[0]);
      MapColumnVector map = (MapColumnVector) destination.cols[2];
      assertFalse(((DateColumnVector) map.keys).isUsingProlepticCalendar());
      assertFalse(((TimestampColumnVector) map.values).isUTC());
      assertTrue(((TimestampColumnVector) map.values).usingProlepticCalendar());
      StructColumnVector struct = (StructColumnVector) destination.cols[3];
      assertTrue(((DateColumnVector) struct.fields[0]).isUsingProlepticCalendar());
      assertEquals(-200000L, ((DateColumnVector) struct.fields[0]).vector[0]);
      UnionColumnVector union = (UnionColumnVector) struct.fields[1];
      assertTrue(((TimestampColumnVector) union.fields[0]).isUTC());
      assertTrue(((TimestampColumnVector) union.fields[0]).usingProlepticCalendar());
      assertFalse(((DateColumnVector) union.fields[1]).isUsingProlepticCalendar());
    }
  }

  @Test
  public void testCachedMetadataPathsFollowReplacementBatchesAndMappedColumns() throws Exception {
    int[] mapping = {3, 0, 1, 2, 1};
    VectorizedRowBatch first = metadataBatch();
    serializer.initialize(first.cols, mapping);
    VectorizedRowBatch schema = mappedMetadataDestination();
    VectorShuffleBatchDeserializer legacy = new VectorShuffleBatchDeserializer(schema, 5);
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(schema, 5);
    byte[] buffer = new byte[4096];
    for (int frame = 0; frame < 3; frame++) {
      // Every frame replaces all vectors, including empty list/map and inactive union children.
      VectorizedRowBatch source = frame == 0 ? first : metadataBatch();
      boolean flipped = frame == 1;
      setMetadataFlags(source, flipped);
      Arrays.fill(buffer, (byte) 0xff);
      int length = serializer.serialize(source, mapping, buffer, 13);
      BytesWritable serialized = new BytesWritable(buffer, 13, length);
      VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
      header.read(serialized, mapping.length, 2);
      assertEquals(flipped ? 0xc8 : 0x37,
          buffer[header.getMetadataOffset()] & 0xff);
      assertEquals(flipped ? 0x09 : 0x06,
          buffer[header.getMetadataOffset() + 1] & 0xff);
      for (boolean accumulate : new boolean[] {false, true}) {
        VectorizedRowBatch destination = mappedMetadataDestination();
        if (accumulate) {
          accumulating.resetAccumulation();
          accumulating.readHeader(serialized);
          accumulating.append(serialized, destination);
        } else {
          legacy.deserialize(serialized, destination);
        }
        assertEquals(source.size, destination.size);
        for (int logical = 0; logical < mapping.length; logical++) {
          assertMetadataEquals(source.cols[mapping[logical]], destination.cols[logical]);
        }
        assertEquals(-20000000000000L,
            ((TimestampColumnVector) destination.cols[2]).time[0]);
        assertEquals(-20000000000000L,
            ((TimestampColumnVector) destination.cols[4]).time[0]);
      }
    }
  }

  @Test
  public void testTimestampMetadataFlagsAcrossByteBoundary() throws Exception {
    VectorizedRowBatch source = new VectorizedRowBatch(9);
    VectorizedRowBatch destination = new VectorizedRowBatch(9);
    for (int column = 0; column < 7; column++) {
      source.cols[column] = new DateColumnVector().setUsingProlepticCalendar(column % 2 == 0);
      destination.cols[column] = new DateColumnVector();
    }
    TimestampColumnVector timestamp = new TimestampColumnVector();
    source.cols[7] = new ListColumnVector(1024, timestamp);
    destination.cols[7] = new ListColumnVector(1024, new TimestampColumnVector());
    source.cols[8] = new DateColumnVector().setUsingProlepticCalendar(true);
    destination.cols[8] = new DateColumnVector();
    source.size = 2;
    destination.reset();
    int[] mapping = identityMap(9);
    serializer.initialize(source.cols, mapping);
    VectorShuffleBatchDeserializer legacy = new VectorShuffleBatchDeserializer(destination, 9);
    VectorShuffleBatchAccumulatingDeserializer accumulating =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 9);
    byte[] buffer = new byte[1024];
    for (int flags = 3; flags >= 0; flags--) {
      timestamp.setIsUTC((flags & 1) != 0);
      timestamp.setUsingProlepticCalendar((flags & 2) != 0);
      Arrays.fill(buffer, (byte) 0xff);
      int length = serializer.serialize(source, mapping, buffer, 11);
      BytesWritable serialized = new BytesWritable(buffer, 11, length);
      VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
      header.read(serialized, 9, 2);
      assertEquals(0x55 | ((flags & 1) << 7), buffer[header.getMetadataOffset()] & 0xff);
      assertEquals(2 | (flags >>> 1), buffer[header.getMetadataOffset() + 1] & 0xff);
      for (boolean accumulate : new boolean[] {false, true}) {
        destination.reset();
        if (accumulate) {
          accumulating.resetAccumulation();
          accumulating.readHeader(serialized);
          accumulating.append(serialized, destination);
        } else {
          legacy.deserialize(serialized, destination);
        }
        TimestampColumnVector actual =
            (TimestampColumnVector) ((ListColumnVector) destination.cols[7]).child;
        assertEquals(timestamp.isUTC(), actual.isUTC());
        assertEquals(timestamp.usingProlepticCalendar(), actual.usingProlepticCalendar());
        assertTrue(((DateColumnVector) destination.cols[8]).isUsingProlepticCalendar());
      }
    }
  }

  private static VectorizedRowBatch mappedMetadataDestination() {
    VectorizedRowBatch template = metadataBatch();
    VectorizedRowBatch destination = new VectorizedRowBatch(5);
    destination.cols = new ColumnVector[] {template.cols[3], template.cols[0], template.cols[1],
        template.cols[2], new TimestampColumnVector()};
    destination.reset();
    return destination;
  }

  private static void setMetadataFlags(VectorizedRowBatch batch, boolean flipped) {
    ((DateColumnVector) ((ListColumnVector) batch.cols[0]).child)
        .setUsingProlepticCalendar(!flipped);
    TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[1];
    timestamp.setIsUTC(!flipped);
    timestamp.setUsingProlepticCalendar(flipped);
    MapColumnVector map = (MapColumnVector) batch.cols[2];
    ((DateColumnVector) map.keys).setUsingProlepticCalendar(flipped);
    ((TimestampColumnVector) map.values).setIsUTC(flipped);
    ((TimestampColumnVector) map.values).setUsingProlepticCalendar(!flipped);
    StructColumnVector struct = (StructColumnVector) batch.cols[3];
    ((DateColumnVector) struct.fields[0]).setUsingProlepticCalendar(!flipped);
    UnionColumnVector union = (UnionColumnVector) struct.fields[1];
    ((TimestampColumnVector) union.fields[0]).setIsUTC(!flipped);
    ((TimestampColumnVector) union.fields[0]).setUsingProlepticCalendar(!flipped);
    ((DateColumnVector) union.fields[1]).setUsingProlepticCalendar(flipped);
  }

  private static void assertMetadataEquals(ColumnVector expected, ColumnVector actual) {
    if (expected instanceof DateColumnVector) {
      assertEquals(((DateColumnVector) expected).isUsingProlepticCalendar(),
          ((DateColumnVector) actual).isUsingProlepticCalendar());
    } else if (expected instanceof TimestampColumnVector) {
      assertEquals(((TimestampColumnVector) expected).isUTC(),
          ((TimestampColumnVector) actual).isUTC());
      assertEquals(((TimestampColumnVector) expected).usingProlepticCalendar(),
          ((TimestampColumnVector) actual).usingProlepticCalendar());
    } else if (expected instanceof ListColumnVector) {
      assertMetadataEquals(((ListColumnVector) expected).child, ((ListColumnVector) actual).child);
    } else if (expected instanceof MapColumnVector) {
      assertMetadataEquals(((MapColumnVector) expected).keys, ((MapColumnVector) actual).keys);
      assertMetadataEquals(((MapColumnVector) expected).values, ((MapColumnVector) actual).values);
    } else {
      ColumnVector[] expectedFields = expected instanceof StructColumnVector
          ? ((StructColumnVector) expected).fields : ((UnionColumnVector) expected).fields;
      ColumnVector[] actualFields = actual instanceof StructColumnVector
          ? ((StructColumnVector) actual).fields : ((UnionColumnVector) actual).fields;
      for (int field = 0; field < expectedFields.length; field++) {
        assertMetadataEquals(expectedFields[field], actualFields[field]);
      }
    }
  }

  @Test
  public void testNestedRepeatingFlagUsesSerializedChildCounts() throws Exception {
    for (int children : new int[] {0, 1, 2}) {
      LongColumnVector child = new LongColumnVector();
      child.isRepeating = true;
      child.vector[0] = 23;
      ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
      list.isRepeating = true;
      list.lengths[0] = children;
      list.childCount = children;
      assertNestedRepeating(batchWithColumn(list, 3), children > 1);
      MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
          new LongColumnVector(), child);
      map.isRepeating = true;
      map.lengths[0] = children;
      map.childCount = children;
      assertNestedRepeating(batchWithColumn(map, 3), children > 1);
    }

    LongColumnVector structChild = new LongColumnVector();
    structChild.isRepeating = true;
    StructColumnVector struct = structOf(structChild);
    struct.noNulls = false;
    struct.isNull[1] = true;
    assertNestedRepeating(batchWithColumn(struct, 2), false);
    struct.isNull[1] = false;
    assertNestedRepeating(batchWithColumn(struct, 2), true);
    struct.isRepeating = true;
    assertNestedRepeating(batchWithColumn(struct, 3), false);

    LongColumnVector unionChild = new LongColumnVector();
    unionChild.isRepeating = true;
    UnionColumnVector union = unionOf(unionChild, new LongColumnVector());
    union.tags[0] = 0;
    union.tags[1] = 1;
    assertNestedRepeating(batchWithColumn(union, 2), false);
    union.tags[1] = 0;
    assertNestedRepeating(batchWithColumn(union, 2), true);
  }

  @Test
  public void testAccumulationOfSingletonRepeatingChildren() throws Exception {
    LongColumnVector listChild = new LongColumnVector();
    listChild.isRepeating = true;
    listChild.vector[0] = 23;
    ListColumnVector list = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, listChild);
    list.lengths[0] = 1;
    list.childCount = 1;
    LongColumnVector mapValue = new LongColumnVector();
    mapValue.isRepeating = true;
    mapValue.vector[0] = 23;
    MapColumnVector map = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector(), mapValue);
    map.lengths[0] = 1;
    map.childCount = 1;
    LongColumnVector structField = new LongColumnVector();
    structField.isRepeating = true;
    structField.vector[0] = 23;
    StructColumnVector struct = structOf(structField);
    struct.noNulls = false;
    struct.isNull[1] = true;
    LongColumnVector unionField = new LongColumnVector();
    unionField.isRepeating = true;
    unionField.vector[0] = 23;
    UnionColumnVector union = unionOf(unionField, new LongColumnVector());
    union.tags[1] = 1;
    VectorizedRowBatch source = new VectorizedRowBatch(4);
    source.cols = new ColumnVector[] {list, map, struct, union};
    source.size = 2;
    VectorizedRowBatch destination = new VectorizedRowBatch(4);
    destination.cols[0] = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector());
    destination.cols[1] = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector(), new LongColumnVector());
    destination.cols[2] = structOf(new LongColumnVector());
    destination.cols[3] = unionOf(new LongColumnVector(), new LongColumnVector());
    destination.reset();
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 4);
    BytesWritable serialized = serializeColumns(source);
    assertFalse(decoder.readHeader(serialized).hasNestedRepeating());
    decoder.append(serialized, destination);
    assertEquals(source.size, destination.size);
    assertTrue(isCompatible(decoder, serialized));
    decoder.append(serialized, destination);
    assertEquals(4, destination.size);
    ListColumnVector actualList = (ListColumnVector) destination.cols[0];
    MapColumnVector actualMap = (MapColumnVector) destination.cols[1];
    assertEquals(2, actualList.childCount);
    assertEquals(2, actualMap.childCount);
    for (ColumnVector child : new ColumnVector[] {actualList.child, actualMap.values}) {
      assertFalse(child.isRepeating);
      assertEquals(23, ((LongColumnVector) child).vector[0]);
      assertEquals(23, ((LongColumnVector) child).vector[1]);
    }
    ColumnVector actualStructField = ((StructColumnVector) destination.cols[2]).fields[0];
    ColumnVector actualUnionField = ((UnionColumnVector) destination.cols[3]).fields[0];
    for (ColumnVector field : new ColumnVector[] {actualStructField, actualUnionField}) {
      assertFalse(field.isRepeating);
      assertEquals(23, ((LongColumnVector) field).vector[0]);
      assertEquals(23, ((LongColumnVector) field).vector[2]);
    }
    assertTrue(destination.cols[2].isNull[1]);
    assertTrue(destination.cols[2].isNull[3]);
    assertArrayEquals(new int[] {0, 1, 0, 1},
        Arrays.copyOf(((UnionColumnVector) destination.cols[3]).tags, 4));
  }

  @Test
  public void testReadersRejectNegativeRowCount() throws Exception {
    BytesWritable serialized = serialize(batchWithColumn(new LongColumnVector(), 2));
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialized, 1, 0);
    ByteBuffer.wrap(serialized.getBytesRaw()).order(ByteOrder.nativeOrder())
        .putInt(header.getRepeatingEnd(), -1);
    assertThrows(IOException.class, () -> deserializer.deserialize(serialized,
        batchWithColumn(new LongColumnVector(), 0)));
    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    assertThrows(IOException.class, () -> decoder.readHeader(serialized));
    assertThrows(IOException.class, () -> decoder.deserialize(serialized, destination));
    assertEquals(0, destination.size);
  }

  @Test
  public void testReadersRejectMismatchedRepeatingSectionLength() throws Exception {
    LongColumnVector repeated = new LongColumnVector();
    repeated.isRepeating = true;
    repeated.vector[0] = 7;
    BytesWritable serialized = serialize(batchWithColumn(repeated, 2));
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialized, 1, 0);
    ByteBuffer.wrap(serialized.getBytesRaw()).order(ByteOrder.nativeOrder())
        .putInt(header.getRepeatingStart() - Integer.BYTES,
            header.getRepeatingEnd() - header.getRepeatingStart() - 1);
    // Keep the relocated partition header valid so decoding reaches the section-end check.
    ByteBuffer.wrap(serialized.getBytesRaw()).order(ByteOrder.nativeOrder())
        .putInt(header.getRepeatingEnd() - 1, 2)
        .put(header.getRepeatingEnd() - 1 + Integer.BYTES, (byte) 0);
    assertThrows(IOException.class, () -> deserializer.deserialize(serialized,
        batchWithColumn(new LongColumnVector(), 0)));
    VectorizedRowBatch destination = batchWithColumn(new LongColumnVector(), 0);
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(destination, 1);
    assertThrows(IOException.class, () -> decoder.deserialize(serialized, destination));
    destination.reset();
    decoder.readHeader(serialized);
    assertThrows(IOException.class, () -> decoder.append(serialized, destination));
  }

  private void assertNestedRepeating(VectorizedRowBatch source, boolean expected) throws Exception {
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialize(source), 1,
        new VectorShuffleBatchMetadata(source.cols, 1).getByteCount());
    assertEquals(expected, header.hasNestedRepeating());
  }

  private BytesWritable serializeColumns(VectorizedRowBatch source) {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, identityMap(source.numCols));
    int length = serializer.serialize(source, identityMap(source.numCols), buffer, 0);
    return new BytesWritable(buffer, 0, length);
  }

  private static VectorizedRowBatch metadataBatch() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    batch.cols[0] = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new DateColumnVector().setUsingProlepticCalendar(true));
    TimestampColumnVector timestamp = new TimestampColumnVector();
    timestamp.setIsUTC(true);
    timestamp.isRepeating = true;
    timestamp.time[0] = -20000000000000L;
    timestamp.nanos[0] = 123456789;
    batch.cols[1] = timestamp;
    TimestampColumnVector mapValues = new TimestampColumnVector();
    mapValues.setUsingProlepticCalendar(true);
    batch.cols[2] = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new DateColumnVector(), mapValues);
    TimestampColumnVector unionTimestamp = new TimestampColumnVector();
    unionTimestamp.setIsUTC(true);
    unionTimestamp.setUsingProlepticCalendar(true);
    DateColumnVector structDate = new DateColumnVector().setUsingProlepticCalendar(true);
    structDate.vector[0] = -200000;
    structDate.vector[1] = -199999;
    batch.cols[3] = structOf(structDate, unionOf(unionTimestamp, new DateColumnVector()));
    batch.size = 2;
    return batch;
  }

  @Test
  public void testSerializerRejectsNegativeUnionTag() {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = -1;
    assertThrows(IllegalArgumentException.class,
        () -> serialize(batchWithColumn(union, 1)));
  }

  @Test
  public void testSerializerRejectsUnionTagBeyondFieldCount() {
    UnionColumnVector union = unionOf(new LongColumnVector());
    union.tags[0] = 1;
    assertThrows(IllegalArgumentException.class,
        () -> serialize(batchWithColumn(union, 1)));
  }

  @Test
  public void testDeserializerRejectsInvalidEncodedUnionTag() throws Exception {
    BytesWritable invalid = serialize(batchWithColumn(unionOf(new LongColumnVector()), 1));
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(invalid, 1, 0);
    ByteBuffer.wrap(invalid.getBytesRaw()).order(ByteOrder.nativeOrder())
        .putInt(header.getOrdinaryStart() + 1, 1);
    assertThrows(IOException.class, () -> deserializer.deserialize(invalid,
        batchWithColumn(unionOf(new LongColumnVector()), 0)));
  }

  @Test
  public void testDeserializerRejectsTruncatedUnionTagSequence() throws Exception {
    BytesWritable serialized = serialize(batchWithColumn(unionOf(new LongColumnVector()), 1));
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(serialized, 1, 0);
    BytesWritable truncated = new BytesWritable(serialized.getBytesRaw(), serialized.getOffset(),
        header.getOrdinaryStart() + 1 + 2 - serialized.getOffset());
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
    BytesWritable serialized = serialize(source);

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

  private void assertRandomBytesSliceRoundTrips(long seed, int iterations) throws Exception {
    Random random = new Random(seed);
    for (int iteration = 0; iteration < iterations; iteration++) {
      RandomBatchLayout layout = randomBatchLayout(random, iteration);
      List<byte[]> expected = randomSliceValues(random, layout.size, iteration);
      BytesColumnVector source = new BytesColumnVector();
      applySliceValues(source, layout, expected, random, iteration);

      for (boolean accumulate : new boolean[] {false, true}) {
        VectorizedRowBatch destination = batchWithColumn(stringAdapter().createVector(), 0);
        roundTrip(layout.batchWithColumn(source), new int[] {0}, destination, accumulate);
        String context = "bytes slice seed=" + seed + ", iteration=" + iteration + ", " + layout
            + ", accumulate=" + accumulate;
        assertEquals(context, layout.size, destination.size);
        assertFalse(context, destination.selectedInUse);
        assertBytesColumnEquals(expected, (BytesColumnVector) destination.cols[0], context);
      }
    }
  }

  private void assertRandomColumnMappingRoundTrips(long seed, int iterations) throws Exception {
    Random random = new Random(seed);
    for (int iteration = 0; iteration < iterations; iteration++) {
      RandomBatchLayout layout = randomBatchLayout(random, iteration);
      int sourceColumnCount = randomMappingSourceColumnCount(random, iteration);
      int[] sourceColumnMap = randomSourceColumnMap(random, iteration, sourceColumnCount);
      VectorizedRowBatch source = randomMappingSourceBatch(layout, sourceColumnCount);
      for (boolean accumulate : new boolean[] {false, true}) {
        VectorizedRowBatch destination = mappingDestinationBatch(sourceColumnMap);
        roundTrip(source, sourceColumnMap, destination, accumulate);
        assertColumnMappingRoundTrip(layout, sourceColumnMap, destination,
            "column mapping seed=" + seed + ", iteration=" + iteration + ", " + layout
                + ", sourceColumnMap=" + Arrays.toString(sourceColumnMap)
                + ", accumulate=" + accumulate);
      }
    }
  }

  private static List<byte[]> randomSliceValues(Random random, int size, int iteration) {
    List<byte[]> values = new ArrayList<>(size);
    for (int logical = 0; logical < size; logical++) {
      int length;
      if (iteration < 7) {
        int[] forcedLengths = {0, 0, 1, 5, 17, 24, 127};
        length = forcedLengths[iteration];
      } else if (logical < INTERESTING_STRINGS.length) {
        values.add(INTERESTING_STRINGS[logical]);
        continue;
      } else {
        length = random.nextInt(160);
      }
      byte[] value = new byte[length];
      for (int index = 0; index < value.length; index++) {
        value[index] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
      }
      values.add(value);
    }
    return values;
  }

  private static void applySliceValues(BytesColumnVector source, RandomBatchLayout layout,
      List<byte[]> expected, Random random, int iteration) {
    if (expected.isEmpty()) {
      return;
    }

    if (iteration == 5 || iteration == 6 || random.nextInt(5) == 0) {
      applySharedSliceValues(source, layout, expected, random, iteration);
      return;
    }

    for (int logical = 0; logical < expected.size(); logical++) {
      byte[] value = expected.get(logical);
      int prefixLength = forcedPrefixLength(iteration, random);
      int suffixLength = iteration == 3 ? 0 : random.nextInt(8);
      byte[] backing = sliceBackingArray(value, prefixLength, suffixLength, logical);
      source.setRef(layout.sourceIndex(logical), backing, prefixLength, value.length);
    }
  }

  private static void applySharedSliceValues(BytesColumnVector source, RandomBatchLayout layout,
      List<byte[]> expected, Random random, int iteration) {
    int[] starts = new int[expected.size()];
    int totalLength = 1;
    for (int logical = 0; logical < expected.size(); logical++) {
      totalLength += forcedPrefixLength(iteration, random);
      starts[logical] = totalLength;
      totalLength += expected.get(logical).length + 1;
    }

    byte[] shared = new byte[totalLength];
    Arrays.fill(shared, (byte) '#');
    for (int logical = 0; logical < expected.size(); logical++) {
      byte[] value = expected.get(logical);
      System.arraycopy(value, 0, shared, starts[logical], value.length);
      source.setRef(layout.sourceIndex(logical), shared, starts[logical], value.length);
    }
  }

  private static int forcedPrefixLength(int iteration, Random random) {
    switch (iteration) {
    case 1:
      return 3;
    case 2:
      return 0;
    case 3:
    case 4:
      return 5;
    default:
      return random.nextInt(8);
    }
  }

  private static byte[] sliceBackingArray(byte[] value, int prefixLength, int suffixLength,
      int logical) {
    byte[] backing = new byte[prefixLength + value.length + suffixLength];
    Arrays.fill(backing, (byte) ('A' + logical % 26));
    System.arraycopy(value, 0, backing, prefixLength, value.length);
    return backing;
  }

  private static void assertBytesColumnEquals(List<byte[]> expected, BytesColumnVector actual,
      String context) {
    for (int logical = 0; logical < expected.size(); logical++) {
      assertBytesValueEquals(context + ", logicalRow=" + logical, expected.get(logical), actual,
          logical);
    }
  }

  private static int randomMappingSourceColumnCount(Random random, int iteration) {
    if (iteration < 6) {
      return iteration == 5 ? 5 : 4;
    }
    return 1 + random.nextInt(8);
  }

  private static int[] randomSourceColumnMap(Random random, int iteration, int sourceColumnCount) {
    switch (iteration) {
    case 0:
      return identityMap(sourceColumnCount);
    case 1:
      return reverseMap(sourceColumnCount);
    case 2:
      return new int[] {sourceColumnCount - 1};
    case 3:
      return new int[] {0, sourceColumnCount - 1};
    case 4:
      return new int[] {2, 0, 3};
    case 5:
      return new int[] {1, 1, 4, 0};
    default:
      int length = 1 + random.nextInt(sourceColumnCount * 2);
      int[] sourceColumnMap = new int[length];
      for (int index = 0; index < sourceColumnMap.length; index++) {
        sourceColumnMap[index] = random.nextInt(sourceColumnCount);
      }
      return sourceColumnMap;
    }
  }

  private static int[] identityMap(int length) {
    int[] map = new int[length];
    for (int index = 0; index < map.length; index++) {
      map[index] = index;
    }
    return map;
  }

  private static int[] reverseMap(int length) {
    int[] map = new int[length];
    for (int index = 0; index < map.length; index++) {
      map[index] = length - index - 1;
    }
    return map;
  }

  private static VectorizedRowBatch randomMappingSourceBatch(RandomBatchLayout layout,
      int sourceColumnCount) {
    VectorizedRowBatch source = new VectorizedRowBatch(sourceColumnCount);
    source.size = layout.size;
    if (layout.selected != null) {
      source.selectedInUse = true;
      System.arraycopy(layout.selected, 0, source.selected, 0, layout.size);
    }
    for (int column = 0; column < sourceColumnCount; column++) {
      source.cols[column] = mappingColumnVector(column);
      for (int logical = 0; logical < layout.size; logical++) {
        setMappingValue(source.cols[column], column, layout.sourceIndex(logical));
      }
    }
    return source;
  }

  private static VectorizedRowBatch mappingDestinationBatch(int[] sourceColumnMap) {
    VectorizedRowBatch destination = new VectorizedRowBatch(sourceColumnMap.length);
    for (int column = 0; column < sourceColumnMap.length; column++) {
      destination.cols[column] = mappingColumnVector(sourceColumnMap[column]);
    }
    // Constructors set size to capacity; accumulation must start with an empty batch.
    destination.reset();
    return destination;
  }

  private static ColumnVector mappingColumnVector(int sourceColumn) {
    return sourceColumn % 2 == 0 ? new LongColumnVector() : new DoubleColumnVector();
  }

  private static void setMappingValue(ColumnVector vector, int sourceColumn, int row) {
    if (vector instanceof LongColumnVector) {
      ((LongColumnVector) vector).vector[row] = expectedMappingLongValue(sourceColumn, row);
    } else {
      ((DoubleColumnVector) vector).vector[row] = expectedMappingDoubleValue(sourceColumn, row);
    }
  }

  private static void assertColumnMappingRoundTrip(RandomBatchLayout layout, int[] sourceColumnMap,
      VectorizedRowBatch destination, String context) {
    assertEquals(context, layout.size, destination.size);
    assertFalse(context, destination.selectedInUse);
    assertEquals(context, sourceColumnMap.length, destination.projectionSize);
    for (int column = 0; column < sourceColumnMap.length; column++) {
      assertEquals(context, column, destination.projectedColumns[column]);
      for (int logical = 0; logical < layout.size; logical++) {
        assertMappingValue(context + ", column=" + column + ", logicalRow=" + logical,
            sourceColumnMap[column], layout.sourceIndex(logical), destination.cols[column],
            logical);
      }
    }
  }

  private static void assertMappingValue(String context, int sourceColumn, int sourceRow,
      ColumnVector vector, int destinationRow) {
    if (sourceColumn % 2 == 0) {
      assertEquals(context, expectedMappingLongValue(sourceColumn, sourceRow),
          ((LongColumnVector) vector).vector[destinationRow]);
    } else {
      assertEquals(context, expectedMappingDoubleValue(sourceColumn, sourceRow),
          ((DoubleColumnVector) vector).vector[destinationRow], 0.0);
    }
  }

  private static long expectedMappingLongValue(int sourceColumn, int row) {
    return ((long) sourceColumn << 32) | row;
  }

  private static double expectedMappingDoubleValue(int sourceColumn, int row) {
    return sourceColumn * 1000000.0d + row + 0.25d;
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

        BytesWritable serialized = serialize(scenario.batchLayout.batchWithColumn(sourceColumn));
        String context = typeName + ", seed=" + seed + ", iteration=" + iteration + ", " + scenario;
        for (boolean accumulate : new boolean[] {false, true}) {
          VectorizedRowBatch result = batchWithColumn(adapter.createDestinationVector(), 0);
          if (!accumulate) {
            deserializer.deserialize(serialized, result);
          } else {
            VectorShuffleBatchAccumulatingDeserializer accumulating =
                new VectorShuffleBatchAccumulatingDeserializer(result, 1);
            VectorShuffleBatchHeader header = accumulating.readHeader(serialized);
            if (scenario.size() > 1 && scenario.size() < accumulating.getAppendCapacity()
                && !header.hasNestedRepeating()) {
              accumulating.append(serialized, result);
              assertColumnRoundTrip(expected, scenario, result, adapter, context + ", first append");
              if (scenario.size() <= accumulating.getAppendCapacity() / 2) {
                assertTrue(context, isCompatible(accumulating, serialized));
                accumulating.append(serialized, result);
                assertEquals(context, scenario.size() * 2, result.size);
                for (int logical = 0; logical < scenario.size(); logical++) {
                  int actualIndex = result.cols[0].isRepeating ? 0 : scenario.size() + logical;
                  assertEquals(context, scenario.isNull(logical),
                      isNull(result.cols[0], actualIndex));
                  if (!scenario.isNull(logical)) {
                    adapter.assertValueEquals(context, expected.get(logical), result.cols[0],
                        actualIndex);
                  }
                }
                continue;
              }
            } else {
              accumulating.deserialize(serialized, result);
            }
          }
          assertColumnRoundTrip(expected, scenario, result, adapter,
              context + ", accumulate=" + accumulate);
        }
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
        return formatPrintableAsciiString(value);
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

  private static RandomColumnAdapter<Double> doubleAdapter() {
    return new RandomColumnAdapter<Double>() {
      @Override
      public ColumnVector createVector() {
        return new DoubleColumnVector();
      }

      @Override
      public Double randomValue(Random random, Double previous, int logical) {
        if (logical < INTERESTING_DOUBLES.length) {
          return INTERESTING_DOUBLES[logical];
        }
        return Double.longBitsToDouble(random.nextLong());
      }

      @Override
      public void setValue(ColumnVector vector, int index, Double value) {
        ((DoubleColumnVector) vector).vector[index] = value;
      }

      @Override
      public void assertValueEquals(String context, Double expected, ColumnVector vector,
          int index) {
        assertEquals(context, Double.doubleToLongBits(expected),
            Double.doubleToLongBits(((DoubleColumnVector) vector).vector[index]));
      }

      @Override
      public String formatValue(Double value) {
        return String.valueOf(value);
      }
    };
  }

  private static RandomColumnAdapter<Long> dateAdapter(boolean proleptic) {
    return new RandomColumnAdapter<Long>() {
      @Override
      public ColumnVector createVector() {
        return new DateColumnVector().setUsingProlepticCalendar(proleptic);
      }

      @Override
      public ColumnVector createDestinationVector() {
        return new DateColumnVector().setUsingProlepticCalendar(!proleptic);
      }

      @Override
      public Long randomValue(Random random, Long previous, int logical) {
        if (logical == 0) {
          return -719162L;
        } else if (logical == 1) {
          return -141428L;
        } else if (logical == 2) {
          return 0L;
        } else if (logical == 3) {
          return 20000L;
        }
        return random.nextInt(400000) - 200000L;
      }

      @Override
      public void setValue(ColumnVector vector, int index, Long value) {
        ((DateColumnVector) vector).vector[index] = value;
      }

      @Override
      public void assertValueEquals(String context, Long expected, ColumnVector vector, int index) {
        assertEquals(context, expected.longValue(), ((DateColumnVector) vector).vector[index]);
      }

      @Override
      public void assertColumnInvariants(String context, List<Long> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        assertEquals(context, proleptic, ((DateColumnVector) vector).isUsingProlepticCalendar());
      }

      @Override
      public String formatValue(Long value) {
        return String.valueOf(value);
      }
    };
  }

  private static RandomColumnAdapter<TimestampValue> timestampAdapter(boolean isUtc,
      boolean proleptic) {
    return new RandomColumnAdapter<TimestampValue>() {
      @Override
      public ColumnVector createVector() {
        TimestampColumnVector vector = new TimestampColumnVector();
        vector.setIsUTC(isUtc);
        vector.setUsingProlepticCalendar(proleptic);
        return vector;
      }

      @Override
      public ColumnVector createDestinationVector() {
        TimestampColumnVector vector = new TimestampColumnVector();
        vector.setIsUTC(!isUtc);
        vector.setUsingProlepticCalendar(!proleptic);
        return vector;
      }

      @Override
      public TimestampValue randomValue(Random random, TimestampValue previous, int logical) {
        final long seconds;
        final int nanos;
        if (logical == 0) {
          seconds = -62135596800L;
          nanos = 999999999;
        } else if (logical == 1) {
          seconds = -1;
          nanos = 1;
        } else if (logical == 2) {
          seconds = 0;
          nanos = 0;
        } else if (logical == 3) {
          seconds = 253402300799L;
          nanos = 123456789;
        } else {
          seconds = random.nextLong() % 3000000000000L;
          nanos = random.nextInt(1000000000);
        }
        return new TimestampValue(seconds * 1000 + nanos / 1000000, nanos);
      }

      @Override
      public void setValue(ColumnVector vector, int index, TimestampValue value) {
        TimestampColumnVector timestamp = (TimestampColumnVector) vector;
        timestamp.time[index] = value.time;
        timestamp.nanos[index] = value.nanos;
      }

      @Override
      public void assertValueEquals(String context, TimestampValue expected, ColumnVector vector,
          int index) {
        TimestampColumnVector timestamp = (TimestampColumnVector) vector;
        assertEquals(context, expected.time, timestamp.time[index]);
        assertEquals(context, expected.nanos, timestamp.nanos[index]);
      }

      @Override
      public void assertColumnInvariants(String context, List<TimestampValue> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        TimestampColumnVector timestamp = (TimestampColumnVector) vector;
        assertEquals(context, isUtc, timestamp.isUTC());
        assertEquals(context, proleptic, timestamp.usingProlepticCalendar());
      }

      @Override
      public String formatValue(TimestampValue value) {
        return value.time + ":" + value.nanos;
      }
    };
  }

  private static RandomColumnAdapter<IntervalDayTimeValue> intervalDayTimeAdapter() {
    return new RandomColumnAdapter<IntervalDayTimeValue>() {
      @Override
      public ColumnVector createVector() {
        return new IntervalDayTimeColumnVector();
      }

      @Override
      public IntervalDayTimeValue randomValue(Random random, IntervalDayTimeValue previous,
          int logical) {
        if (logical == 0) {
          return new IntervalDayTimeValue(Long.MIN_VALUE + 1, -999999999);
        } else if (logical == 1) {
          return new IntervalDayTimeValue(-1, -1);
        } else if (logical == 2) {
          return new IntervalDayTimeValue(0, 0);
        } else if (logical == 3) {
          return new IntervalDayTimeValue(Long.MAX_VALUE - 1, 123456789);
        }
        long seconds = random.nextLong();
        int nanos = random.nextInt(1000000000);
        return new IntervalDayTimeValue(seconds, seconds < 0 ? -nanos : nanos);
      }

      @Override
      public void setValue(ColumnVector vector, int index, IntervalDayTimeValue value) {
        ((IntervalDayTimeColumnVector) vector).set(index,
            new HiveIntervalDayTime(value.totalSeconds, value.nanos));
      }

      @Override
      public void assertValueEquals(String context, IntervalDayTimeValue expected,
          ColumnVector vector, int index) {
        IntervalDayTimeColumnVector interval = (IntervalDayTimeColumnVector) vector;
        assertEquals(context, expected.totalSeconds, interval.getTotalSeconds(index));
        assertEquals(context, expected.nanos, interval.getNanos(index));
      }

      @Override
      public String formatValue(IntervalDayTimeValue value) {
        return value.totalSeconds + ":" + value.nanos;
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

  private static <K, V> RandomColumnAdapter<MapValue<K, V>> mapAdapter(
      RandomColumnAdapter<K> keyAdapter, RandomColumnAdapter<V> valueAdapter) {
    return new RandomColumnAdapter<MapValue<K, V>>() {
      @Override
      public ColumnVector createVector() {
        return new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, keyAdapter.createVector(),
            valueAdapter.createVector());
      }

      @Override
      public MapValue<K, V> randomValue(Random random, MapValue<K, V> previous, int logical) {
        if (previous != null && random.nextInt(10) == 0) {
          return previous;
        }
        int choice = random.nextInt(100);
        int length = choice < 15 ? 0 : choice < 25 ? 1 : random.nextInt(MAX_MAP_LENGTH + 1);
        List<MapEntryValue<K, V>> entries = new ArrayList<>(length);
        K previousKey = null;
        V previousValue = null;
        for (int entry = 0; entry < length; entry++) {
          K key = keyAdapter.randomValue(random, previousKey, entry);
          boolean valueIsNull = random.nextInt(5) == 0;
          V value = valueIsNull ? null : valueAdapter.randomValue(random, previousValue, entry);
          entries.add(new MapEntryValue<>(key, new NullableValue<>(valueIsNull, value)));
          previousKey = key;
          if (!valueIsNull) {
            previousValue = value;
          }
        }
        return new MapValue<>(random.nextInt(4), entries);
      }

      @Override
      public void setValue(ColumnVector vector, int index, MapValue<K, V> value) {
        MapColumnVector map = (MapColumnVector) vector;
        int offset = map.childCount + value.gapBefore;
        map.offsets[index] = offset;
        map.lengths[index] = value.entries.size();
        map.keys.ensureSize(offset + value.entries.size(), true);
        map.values.ensureSize(offset + value.entries.size(), true);
        for (int entry = 0; entry < value.entries.size(); entry++) {
          MapEntryValue<K, V> expected = value.entries.get(entry);
          int childIndex = offset + entry;
          keyAdapter.setValue(map.keys, childIndex, expected.key);
          if (expected.value.isNull) {
            setNull(map.values, childIndex);
          } else {
            valueAdapter.setValue(map.values, childIndex, expected.value.value);
          }
        }
        map.childCount = offset + value.entries.size();
      }

      @Override
      public void assertValueEquals(String context, MapValue<K, V> expected, ColumnVector vector,
          int index) {
        MapColumnVector map = (MapColumnVector) vector;
        assertEquals(context, expected.entries.size(), map.lengths[index]);
        int offset = Math.toIntExact(map.offsets[index]);
        for (int entry = 0; entry < expected.entries.size(); entry++) {
          MapEntryValue<K, V> expectedEntry = expected.entries.get(entry);
          int childIndex = offset + entry;
          String entryContext = context + ", entry=" + entry;
          assertFalse(entryContext + ", key", isNull(map.keys, childIndex));
          keyAdapter.assertValueEquals(entryContext + ", key", expectedEntry.key, map.keys,
              childIndex);
          boolean actualValueIsNull = isNull(map.values, childIndex);
          assertEquals(entryContext + ", value", expectedEntry.value.isNull, actualValueIsNull);
          if (!actualValueIsNull) {
            valueAdapter.assertValueEquals(entryContext + ", value", expectedEntry.value.value,
                map.values, childIndex);
          }
        }
      }

      @Override
      public String formatValue(MapValue<K, V> value) {
        StringBuilder formatted = new StringBuilder("MAP{");
        for (int entry = 0; entry < value.entries.size(); entry++) {
          if (entry > 0) {
            formatted.append(", ");
          }
          MapEntryValue<K, V> expected = value.entries.get(entry);
          formatted.append(keyAdapter.formatValue(expected.key)).append(" -> ")
              .append(expected.value.isNull ? "NULL"
                  : valueAdapter.formatValue(expected.value.value));
        }
        return formatted.append('}').toString();
      }

      @Override
      public void assertColumnInvariants(String context, List<MapValue<K, V>> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        MapColumnVector map = (MapColumnVector) vector;
        int expectedChildCount = 0;
        for (int logical = 0; logical < scenario.valueCount(); logical++) {
          int index = map.isRepeating ? 0 : logical;
          assertEquals(context + ", logicalRow=" + logical, expectedChildCount, map.offsets[index]);
          int expectedLength = scenario.isNull(logical) ? 0 : expected.get(logical).entries.size();
          assertEquals(context + ", logicalRow=" + logical, expectedLength, map.lengths[index]);
          expectedChildCount += expectedLength;
        }
        assertEquals(context, expectedChildCount, map.childCount);
      }
    };
  }

  private static RandomColumnAdapter<StructValue> structAdapter(
      RandomColumnAdapter<?>... fieldAdapters) {
    return new RandomColumnAdapter<StructValue>() {
      @Override
      public ColumnVector createVector() {
        ColumnVector[] fields = new ColumnVector[fieldAdapters.length];
        for (int field = 0; field < fields.length; field++) {
          fields[field] = fieldAdapters[field].createVector();
        }
        return structOf(fields);
      }

      @Override
      public StructValue randomValue(Random random, StructValue previous, int logical) {
        if (previous != null && random.nextInt(10) == 0) {
          return previous;
        }
        List<NullableValue<?>> fields = new ArrayList<>(fieldAdapters.length);
        for (int field = 0; field < fieldAdapters.length; field++) {
          boolean isNull = random.nextInt(5) == 0;
          Object previousValue = previous == null || previous.fields.get(field).isNull
              ? null : previous.fields.get(field).value;
          fields.add(new NullableValue<>(isNull, isNull ? null
              : randomAdapterValue(fieldAdapters[field], random, previousValue, logical)));
        }
        return new StructValue(fields);
      }

      @Override
      public void setValue(ColumnVector vector, int index, StructValue value) {
        StructColumnVector struct = (StructColumnVector) vector;
        for (int field = 0; field < fieldAdapters.length; field++) {
          NullableValue<?> expected = value.fields.get(field);
          if (expected.isNull) {
            setNull(struct.fields[field], index);
          } else {
            setAdapterValue(fieldAdapters[field], struct.fields[field], index, expected.value);
          }
        }
      }

      @Override
      public void assertValueEquals(String context, StructValue expected, ColumnVector vector,
          int index) {
        StructColumnVector struct = (StructColumnVector) vector;
        assertEquals(context, fieldAdapters.length, struct.fields.length);
        for (int field = 0; field < fieldAdapters.length; field++) {
          NullableValue<?> expectedField = expected.fields.get(field);
          int fieldIndex = struct.fields[field].isRepeating ? 0 : index;
          boolean actualIsNull = isNull(struct.fields[field], fieldIndex);
          String fieldContext = context + ", field=" + field;
          assertEquals(fieldContext, expectedField.isNull, actualIsNull);
          if (!actualIsNull) {
            assertAdapterValue(fieldAdapters[field], fieldContext, expectedField.value,
                struct.fields[field], fieldIndex);
          }
        }
      }

      @Override
      public String formatValue(StructValue value) {
        StringBuilder formatted = new StringBuilder("STRUCT{");
        for (int field = 0; field < fieldAdapters.length; field++) {
          if (field > 0) {
            formatted.append(", ");
          }
          NullableValue<?> expected = value.fields.get(field);
          formatted.append('f').append(field).append('=').append(expected.isNull ? "NULL"
              : formatAdapterValue(fieldAdapters[field], expected.value));
        }
        return formatted.append('}').toString();
      }
    };
  }

  private static RandomColumnAdapter<UnionValue> unionAdapter(
      RandomColumnAdapter<?>... fieldAdapters) {
    if (fieldAdapters.length == 0) {
      throw new IllegalArgumentException("A union property adapter requires at least one field");
    }
    return new RandomColumnAdapter<UnionValue>() {
      @Override
      public ColumnVector createVector() {
        ColumnVector[] fields = new ColumnVector[fieldAdapters.length];
        for (int field = 0; field < fields.length; field++) {
          fields[field] = fieldAdapters[field].createVector();
        }
        return unionOf(fields);
      }

      @Override
      public UnionValue randomValue(Random random, UnionValue previous, int logical) {
        if (previous != null && random.nextInt(10) == 0) {
          return previous;
        }
        int tag = logical < fieldAdapters.length ? logical : random.nextInt(fieldAdapters.length);
        boolean isNull = random.nextInt(5) == 0;
        Object previousValue = previous == null || previous.tag != tag || previous.value.isNull
            ? null : previous.value.value;
        return new UnionValue(tag, new NullableValue<>(isNull, isNull ? null
            : randomAdapterValue(fieldAdapters[tag], random, previousValue, logical)));
      }

      @Override
      public void setValue(ColumnVector vector, int index, UnionValue value) {
        UnionColumnVector union = (UnionColumnVector) vector;
        union.tags[index] = value.tag;
        if (value.value.isNull) {
          setNull(union.fields[value.tag], index);
        } else {
          setAdapterValue(fieldAdapters[value.tag], union.fields[value.tag], index,
              value.value.value);
        }
      }

      @Override
      public void assertValueEquals(String context, UnionValue expected, ColumnVector vector,
          int index) {
        UnionColumnVector union = (UnionColumnVector) vector;
        assertEquals(context + ", tag", expected.tag, union.tags[index]);
        ColumnVector field = union.fields[expected.tag];
        int fieldIndex = field.isRepeating ? 0 : index;
        boolean actualIsNull = isNull(field, fieldIndex);
        String fieldContext = context + ", tag=" + expected.tag;
        assertEquals(fieldContext, expected.value.isNull, actualIsNull);
        if (!actualIsNull) {
          assertAdapterValue(fieldAdapters[expected.tag], fieldContext, expected.value.value,
              field, fieldIndex);
        }
      }

      @Override
      public String formatValue(UnionValue value) {
        return "UNION{tag=" + value.tag + ", value=" + (value.value.isNull ? "NULL"
            : formatAdapterValue(fieldAdapters[value.tag], value.value.value)) + "}";
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static Object randomAdapterValue(RandomColumnAdapter<?> adapter, Random random,
      Object previous, int logical) {
    return ((RandomColumnAdapter<Object>) adapter).randomValue(random, previous, logical);
  }

  @SuppressWarnings("unchecked")
  private static void setAdapterValue(RandomColumnAdapter<?> adapter, ColumnVector vector,
      int index, Object value) {
    ((RandomColumnAdapter<Object>) adapter).setValue(vector, index, value);
  }

  @SuppressWarnings("unchecked")
  private static void assertAdapterValue(RandomColumnAdapter<?> adapter, String context,
      Object expected, ColumnVector vector, int index) {
    ((RandomColumnAdapter<Object>) adapter).assertValueEquals(context, expected, vector, index);
  }

  @SuppressWarnings("unchecked")
  private static String formatAdapterValue(RandomColumnAdapter<?> adapter, Object value) {
    return ((RandomColumnAdapter<Object>) adapter).formatValue(value);
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

  private static String formatPrintableAsciiString(byte[] value) {
    StringBuilder formatted = new StringBuilder("\"");
    for (byte rawByte : value) {
      int unsignedByte = rawByte & 0xff;
      if (unsignedByte == '\\') {
        formatted.append("\\\\");
      } else if (unsignedByte == '\"') {
        formatted.append("\\\"");
      } else if (unsignedByte >= 0x20 && unsignedByte <= 0x7e) {
        formatted.append((char) unsignedByte);
      } else {
        formatted.append(String.format("\\x%02x", unsignedByte));
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
    int length = random.nextInt(MAX_STRING_LENGTH + 1);
    byte[] value = new byte[length];
    for (int index = 0; index < length; index++) {
      value[index] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
    }
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

    default ColumnVector createDestinationVector() {
      return createVector();
    }

    T randomValue(Random random, T previous, int logical);

    void setValue(ColumnVector vector, int index, T value);

    void assertValueEquals(String context, T expected, ColumnVector vector, int index);

    String formatValue(T value);

    default void assertColumnInvariants(String context, List<T> expected,
        RandomColumnScenario scenario, ColumnVector vector) {
    }
  }

  private static final class TimestampValue {
    private final long time;
    private final int nanos;

    private TimestampValue(long time, int nanos) {
      this.time = time;
      this.nanos = nanos;
    }
  }

  private static final class IntervalDayTimeValue {
    private final long totalSeconds;
    private final int nanos;

    private IntervalDayTimeValue(long totalSeconds, int nanos) {
      this.totalSeconds = totalSeconds;
      this.nanos = nanos;
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

  private static final class MapEntryValue<K, V> {
    private final K key;
    private final NullableValue<V> value;

    private MapEntryValue(K key, NullableValue<V> value) {
      this.key = key;
      this.value = value;
    }
  }

  private static final class MapValue<K, V> {
    private final int gapBefore;
    private final List<MapEntryValue<K, V>> entries;

    private MapValue(int gapBefore, List<MapEntryValue<K, V>> entries) {
      this.gapBefore = gapBefore;
      this.entries = entries;
    }
  }

  private static final class StructValue {
    private final List<NullableValue<?>> fields;

    private StructValue(List<NullableValue<?>> fields) {
      this.fields = fields;
    }
  }

  private static final class UnionValue {
    private final int tag;
    private final NullableValue<?> value;

    private UnionValue(int tag, NullableValue<?> value) {
      this.tag = tag;
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

  private static StructColumnVector structOf(ColumnVector... fields) {
    return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  private static UnionColumnVector unionOf(ColumnVector... fields) {
    return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  private static boolean isCompatible(VectorShuffleBatchAccumulatingDeserializer decoder,
      BytesWritable serialized) throws IOException {
    decoder.readHeader(serialized);
    return decoder.isCompatible();
  }

  private BytesWritable serialize(VectorizedRowBatch source) throws Exception {
    BytesWritable serialized = new BytesWritable();
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, new int[] {0});
    int length = serializer.serialize(source, new int[] {0}, buffer, 0);
    serialized.set(buffer, 0, length);
    return serialized;
  }

  private void roundTrip(VectorizedRowBatch source, int[] sourceColumnMap,
      VectorizedRowBatch destination) throws Exception {
    roundTrip(source, sourceColumnMap, destination, false);
  }

  private void roundTrip(VectorizedRowBatch source, int[] sourceColumnMap,
      VectorizedRowBatch destination, boolean accumulate) throws Exception {
    BytesWritable serialized = new BytesWritable();
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, sourceColumnMap);
    int length = serializer.serialize(source, sourceColumnMap, buffer, 0);
    serialized.set(buffer, 0, length);
    if (!accumulate) {
      deserializer.deserialize(serialized, destination);
    } else {
      VectorShuffleBatchAccumulatingDeserializer decoder =
          new VectorShuffleBatchAccumulatingDeserializer(destination, sourceColumnMap.length);
      VectorShuffleBatchHeader header = decoder.readHeader(serialized);
      if (source.size > 1 && source.size < decoder.getAppendCapacity()
          && !header.hasNestedRepeating()) {
        decoder.append(serialized, destination);
      } else {
        decoder.deserialize(serialized, destination);
      }
    }
  }

  private static void assertBytesValueEquals(byte[] expected, BytesColumnVector actual,
      int index) {
    assertBytesValueEquals(null, expected, actual, index);
  }

  private static void assertBytesValueEquals(String context, byte[] expected,
      BytesColumnVector actual, int index) {
    assertArrayEquals(context, expected, Arrays.copyOfRange(actual.vector[index],
        actual.start[index], actual.start[index] + actual.length[index]));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
