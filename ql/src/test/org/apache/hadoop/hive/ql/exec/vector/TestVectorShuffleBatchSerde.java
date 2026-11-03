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

import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_BIGINT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_DECIMAL_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_STRING_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ARRAY_UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.BIGINT_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.BIGINT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.BYTES_SLICE_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.BYTES_SLICE_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.COLUMN_MAPPING_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.COLUMN_MAPPING_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.COMPLEX_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.COMPLEX_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DATE_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DECIMAL_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DOUBLE_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.INTERVAL_DAY_TIME_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.MAP_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_ARRAY_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_ARRAY_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NESTED_UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NestedRepeatingCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.RandomColumnAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.SERIALIZE_BUFFER_SIZE;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRING_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_WITH_UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.TIMESTAMP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_WITH_STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.VOID_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.arrayAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertBytesColumnEquals;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertColumnMappingRoundTrip;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertColumnRoundTrip;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertNestedRepeatingBatch;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.batchWithColumn;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.bigIntAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.dateAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.decimalAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.doubleAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachBytesSliceCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachColumnCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachColumnMappingCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachNestedRepeatingCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.identityMap;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.intervalDayTimeAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.mapAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.mappingDestinationBatch;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.nestedRepeatingDestination;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.stringAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.structAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.timestampAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.unionAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.voidAdapter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

/** Tests frame serialization and the ordinary deserializer with seeded properties. */
public class TestVectorShuffleBatchSerde {
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
    for (boolean sourceDecimal64 : new boolean[] {false, true}) {
      for (boolean destinationDecimal64 : new boolean[] {false, true}) {
        assertRandomColumnRoundTrips("DECIMAL_SOURCE_64_" + sourceDecimal64
            + "_DESTINATION_64_" + destinationDecimal64, DECIMAL_PROPERTY_TEST_SEED,
            PROPERTY_TEST_ITERATIONS, decimalAdapter(sourceDecimal64, destinationDecimal64));
      }
    }
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
    assertRandomColumnRoundTrips("VOID", VOID_PROPERTY_TEST_SEED, PROPERTY_TEST_ITERATIONS,
        voidAdapter());
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
  public void testNestedRepeatingColumnSchemas() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachNestedRepeatingCase((scenarios, source, expectedNestedRepeating, context) ->
        assertNestedRepeatingRoundTrips(scenarios, source, expectedNestedRepeating, buffer, context));
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

  private <T> void assertRandomColumnRoundTrips(String typeName, long seed, int iterations,
      RandomColumnAdapter<T> adapter) throws Exception {
    forEachColumnCase(typeName, seed, iterations, adapter, "TestVectorShuffleBatchSerde-",
        (expected, scenario, source, context) -> {
          BytesWritable serialized = serialize(source);
          VectorizedRowBatch result = batchWithColumn(adapter.createDestinationVector(), 0);
          deserializer.deserialize(serialized, result);
          assertColumnRoundTrip(expected, scenario, result, adapter,
              context + ", decoder=ordinary");
        });
  }

  private void assertRandomBytesSliceRoundTrips(long seed, int iterations) throws Exception {
    forEachBytesSliceCase(seed, iterations, (layout, source, expected, context) -> {
      VectorizedRowBatch destination = batchWithColumn(stringAdapter().createVector(), 0);
      roundTrip(source, new int[] {0}, destination);
      String decoderContext = context + ", decoder=ordinary";
      assertEquals(decoderContext, layout.size, destination.size);
      assertFalse(decoderContext, destination.selectedInUse);
      assertBytesColumnEquals(expected, (BytesColumnVector) destination.cols[0], decoderContext);
    });
  }

  private void assertRandomColumnMappingRoundTrips(long seed, int iterations) throws Exception {
    forEachColumnMappingCase(seed, iterations, (layout, mapping, source, context) -> {
      VectorizedRowBatch destination = mappingDestinationBatch(mapping);
      roundTrip(source, mapping, destination);
      assertColumnMappingRoundTrip(layout, mapping, destination,
          context + ", decoder=ordinary");
    });
  }

  private void assertNestedRepeatingRoundTrips(NestedRepeatingCase[] scenarios,
      VectorizedRowBatch source, boolean expectedNestedRepeating, byte[] buffer, String context)
      throws Exception {
    int[] mapping = identityMap(scenarios.length);
    serializer.initialize(source.cols, mapping);
    int length = serializer.serialize(source, mapping, buffer, 0);
    BytesWritable frame = new BytesWritable(buffer, 0, length);
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(frame, scenarios.length,
        new VectorShuffleBatchMetadata(source.cols, scenarios.length).getByteCount());
    assertEquals(context, expectedNestedRepeating, header.hasNestedRepeating());
    VectorizedRowBatch destination = nestedRepeatingDestination(scenarios);
    deserializer.deserialize(frame, destination);
    assertNestedRepeatingBatch(scenarios, destination, 1, true, context + ", ordinary");
  }

  private void roundTrip(VectorizedRowBatch source, int[] sourceColumnMap,
      VectorizedRowBatch destination) throws Exception {
    BytesWritable serialized = new BytesWritable();
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, sourceColumnMap);
    int length = serializer.serialize(source, sourceColumnMap, buffer, 0);
    serialized.set(buffer, 0, length);
    deserializer.deserialize(serialized, destination);
  }

  private BytesWritable serialize(VectorizedRowBatch source) throws Exception {
    BytesWritable serialized = new BytesWritable();
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, new int[] {0});
    int length = serializer.serialize(source, new int[] {0}, buffer, 0);
    serialized.set(buffer, 0, length);
    return serialized;
  }
}
