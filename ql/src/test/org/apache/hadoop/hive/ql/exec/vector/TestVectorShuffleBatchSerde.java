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
  private static final int ARRAY_PROPERTY_TEST_ITERATIONS = 1000;
  private static final int NESTED_ARRAY_PROPERTY_TEST_ITERATIONS = 500;
  private static final int COMPLEX_PROPERTY_TEST_ITERATIONS = 500;
  private static final int NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS = 200;
  private static final int MAX_STRING_LENGTH = 24;
  private static final int MAX_ARRAY_LENGTH = 16;
  private static final int MAX_MAP_LENGTH = 16;
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
}
