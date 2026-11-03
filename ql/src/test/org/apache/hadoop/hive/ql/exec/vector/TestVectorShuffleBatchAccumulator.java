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
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.CompatibleFrameSequence;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.CompatibleSequenceScenario;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DATE_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DECIMAL_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.DOUBLE_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.ExpectedSequenceBatch;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.GeneratedFrame;
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
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.NullableValue;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.PROPERTY_TEST_ITERATIONS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.RandomColumnAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.SERIALIZE_BUFFER_SIZE;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRING_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.STRUCT_WITH_UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.SequenceFlushReason;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.TIMESTAMP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_MAP_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.UNION_WITH_STRUCT_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.VOID_PROPERTY_TEST_SEED;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.arrayAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertBytesColumnEquals;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertColumnMappingRoundTrip;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertColumnRoundTrip;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertCompatibleFrameSequenceResult;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.assertNestedRepeatingBatch;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.batchWithColumn;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.bigIntAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.dateAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.decimalAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.doubleAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachBytesSliceCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachColumnCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachColumnMappingCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachCompatibleFrameSequence;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.forEachNestedRepeatingCase;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.identityMap;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.intervalDayTimeAdapter;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchTestSupport.isNull;
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

/** Tests the accumulating decoder and receiver batch lifecycle with seeded properties. */
public class TestVectorShuffleBatchAccumulator {
  private static final long COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED = 0xC04A71B1E5L;
  private final VectorShuffleBatchSerializer serializer = new VectorShuffleBatchSerializer();

  // Accumulating decoder properties.

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
    forEachColumnCase(typeName, seed, iterations, adapter, "TestVectorShuffleBatchAccumulator-",
        (expected, scenario, source, context) -> {
          BytesWritable serialized = serialize(source);
          VectorizedRowBatch result = batchWithColumn(adapter.createDestinationVector(), 0);
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
              return;
            }
          } else {
            accumulating.deserialize(serialized, result);
          }
          assertColumnRoundTrip(expected, scenario, result, adapter,
              context + ", decoder=accumulating");
        });
  }

  private void assertRandomBytesSliceRoundTrips(long seed, int iterations) throws Exception {
    forEachBytesSliceCase(seed, iterations, (layout, source, expected, context) -> {
      VectorizedRowBatch destination = batchWithColumn(stringAdapter().createVector(), 0);
      roundTrip(source, new int[] {0}, destination);
      String decoderContext = context + ", decoder=accumulating";
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
          context + ", decoder=accumulating");
    });
  }

  private void assertNestedRepeatingRoundTrips(NestedRepeatingCase[] scenarios,
      VectorizedRowBatch source, boolean expectedNestedRepeating, byte[] buffer, String context)
      throws Exception {
    int[] mapping = identityMap(scenarios.length);
    serializer.initialize(source.cols, mapping);
    int length = serializer.serialize(source, mapping, buffer, 0);
    BytesWritable frame = new BytesWritable(buffer, 0, length);
    VectorizedRowBatch result = nestedRepeatingDestination(scenarios);
    VectorShuffleBatchAccumulatingDeserializer decoder =
        new VectorShuffleBatchAccumulatingDeserializer(result, scenarios.length);
    assertEquals(context, expectedNestedRepeating, decoder.readHeader(frame).hasNestedRepeating());
    // Choose the path from the case's independent oracle, never from the decoded flag.
    if (expectedNestedRepeating || source.size == 1) {
      decoder.deserializeWithHeader(frame, result);
      assertNestedRepeatingBatch(scenarios, result, 1, true, context + ", standalone");
    } else {
      assertTrue(context, 2 * source.size <= decoder.getAppendCapacity());
      for (int copies = 1; copies <= 2; copies++) {
        if (copies > 1) {
          assertFalse(context, decoder.readHeader(frame).hasNestedRepeating());
          assertTrue(context, decoder.isCompatible());
        }
        decoder.append(frame, result);
        // Repeated top-level columns retain their representation; appended ordinary columns
        // normalize every nested zero/singleton repeating child, including inactive fields.
        assertNestedRepeatingBatch(scenarios, result, copies, false,
            context + ", append=" + copies);
      }
    }
  }

  private void roundTrip(VectorizedRowBatch source, int[] sourceColumnMap,
      VectorizedRowBatch destination) throws Exception {
    BytesWritable serialized = new BytesWritable();
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    serializer.initialize(source.cols, sourceColumnMap);
    int length = serializer.serialize(source, sourceColumnMap, buffer, 0);
    serialized.set(buffer, 0, length);
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

  // Compatible compact-frame sequences and their independently planned flush boundaries.

  @Test
  public void testCompatibleFrameSequencesFitWithoutAutomaticFlush() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.FITS,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  @Test
  public void testCompatibleFrameSequencesReachRowCapacityExactly() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.ROW_LIMIT_EXACT,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  @Test
  public void testCompatibleFrameSequencesFlushBeforeRowCapacityOverflow() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.ROW_LIMIT_OVERFLOW,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  @Test
  public void testCompatibleFrameSequencesReachByteLimitExactly() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.BYTE_LIMIT_EXACT,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  @Test
  public void testCompatibleFrameSequencesFlushBeforeByteLimitOverflow() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.BYTE_LIMIT_OVERFLOW,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  @Test
  public void testCompatibleFrameSequencesCrossMultipleBoundaries() throws Exception {
    byte[] buffer = new byte[SERIALIZE_BUFFER_SIZE];
    forEachCompatibleFrameSequence(COMPATIBLE_FRAME_SEQUENCE_PROPERTY_TEST_SEED,
        PROPERTY_TEST_ITERATIONS, CompatibleSequenceScenario.MULTIPLE_BOUNDARIES,
        sequence -> assertCompatibleFrameSequence(sequence, buffer));
  }

  private void assertCompatibleFrameSequence(CompatibleFrameSequence sequence, byte[] buffer)
      throws Exception {
    int[] lengths = new int[sequence.frames.size()];
    for (int frame = 0; frame < lengths.length; frame++) {
      lengths[frame] = serializeSequenceFrame(sequence.frames.get(frame), buffer);
    }
    for (ExpectedSequenceBatch expected : sequence.expectedBatches) {
      if (expected.reason == SequenceFlushReason.BYTE_LIMIT
          || expected.reason == SequenceFlushReason.BYTE_OVERFLOW) {
        long target = VectorizedRowBatch.DEFAULT_BYTES
            - (expected.reason == SequenceFlushReason.BYTE_OVERFLOW ? 1L : 0L);
        long originalLength = sequenceBytes(lengths, expected.firstFrame, expected.endFrame);
        assertTrue(sequence.context + ", room for generated STRING padding", originalLength < target);
        int paddedFrame = expected.endFrame - 1;
        sequence.addPadding(paddedFrame, Math.toIntExact(target - originalLength));
        lengths[paddedFrame] = serializeSequenceFrame(sequence.frames.get(paddedFrame), buffer);
        // STRING lengths have fixed-width prefixes, so payload growth changes frame size exactly.
        assertEquals(sequence.context + ", exact serialized-byte target", target,
            sequenceBytes(lengths, expected.firstFrame, expected.endFrame));
      }
    }
    assertSequenceBoundaryPlan(sequence, lengths);
    for (boolean retained : new boolean[] {false, true}) {
      assertCompatibleSequenceAccumulation(sequence, lengths, buffer, retained);
    }
  }

  private int serializeSequenceFrame(GeneratedFrame frame, byte[] buffer) {
    int[] columns = identityMap(frame.source.numCols);
    serializer.initialize(frame.source.cols, columns);
    return serializer.serialize(frame.source, columns, buffer, SEQUENCE_FRAME_OFFSET);
  }

  private static int sequenceRows(CompatibleFrameSequence sequence, int firstFrame, int endFrame) {
    int rows = 0;
    for (int frame = firstFrame; frame < endFrame; frame++) {
      rows += sequence.frames.get(frame).columns.get(0).size();
    }
    return rows;
  }

  private static long sequenceBytes(int[] lengths, int firstFrame, int endFrame) {
    long bytes = 0;
    for (int frame = firstFrame; frame < endFrame; frame++) {
      // Compact inputs use an empty key; the value length is the entire incoming byte charge.
      bytes += lengths[frame];
    }
    return bytes;
  }

  /** Prove each generated boundary before asking the accumulator to process anything. */
  private static void assertSequenceBoundaryPlan(CompatibleFrameSequence sequence, int[] lengths) {
    String context = sequence.context;
    for (int frame = 0; frame < lengths.length; frame++) {
      int rows = sequence.frames.get(frame).source.size;
      assertTrue(context + ", compact frame=" + frame, rows > 1 && rows < sequence.rowCapacity);
      assertTrue(context + ", frame byte budget=" + frame,
          lengths[frame] > 0 && lengths[frame] < VectorizedRowBatch.DEFAULT_BYTES);
    }
    int previousEnd = 0;
    for (ExpectedSequenceBatch expected : sequence.expectedBatches) {
      assertEquals(context + ", contiguous expected batches", previousEnd, expected.firstFrame);
      assertTrue(context + ", nonempty expected batch", expected.firstFrame < expected.endFrame);
      int rows = sequenceRows(sequence, expected.firstFrame, expected.endFrame);
      long bytes = sequenceBytes(lengths, expected.firstFrame, expected.endFrame);
      switch (expected.reason) {
      case FINAL_FLUSH:
        assertTrue(context, rows < sequence.rowCapacity);
        assertTrue(context, bytes < VectorizedRowBatch.DEFAULT_BYTES);
        assertEquals(context, lengths.length, expected.endFrame);
        break;
      case ROW_LIMIT:
        assertEquals(context, sequence.rowCapacity, rows);
        assertTrue(context, bytes < VectorizedRowBatch.DEFAULT_BYTES);
        break;
      case ROW_OVERFLOW:
        assertTrue(context, rows < sequence.rowCapacity);
        assertTrue(context, rows + sequence.frames.get(expected.endFrame).source.size
            > sequence.rowCapacity);
        assertTrue(context, bytes + lengths[expected.endFrame] < VectorizedRowBatch.DEFAULT_BYTES);
        break;
      case BYTE_LIMIT:
        assertEquals(context, (long) VectorizedRowBatch.DEFAULT_BYTES, bytes);
        assertTrue(context, rows < sequence.rowCapacity);
        break;
      case BYTE_OVERFLOW:
        assertEquals(context, VectorizedRowBatch.DEFAULT_BYTES - 1L, bytes);
        assertTrue(context, bytes + lengths[expected.endFrame] > VectorizedRowBatch.DEFAULT_BYTES);
        assertTrue(context, rows + sequence.frames.get(expected.endFrame).source.size
            < sequence.rowCapacity);
        break;
      default:
        throw new AssertionError(expected.reason);
      }
      previousEnd = expected.endFrame;
    }
    assertEquals(context, lengths.length, previousEnd);
  }

  private void assertCompatibleSequenceAccumulation(CompatibleFrameSequence sequence,
      int[] lengths, byte[] buffer, boolean retained) throws Exception {
    String context = sequence.context + ", retained=" + retained;
    Operator<?> reducer = mock(Operator.class);
    VectorizedRowBatchCtx batchContext = mock(VectorizedRowBatchCtx.class);
    List<VectorizedRowBatch> allocations = new ArrayList<>();
    Supplier<VectorizedRowBatch> factory = () -> {
      VectorizedRowBatch batch = sequence.newDestination();
      if (!allocations.isEmpty()) {
        assertNotSame(context, allocations.get(allocations.size() - 1), batch);
      }
      allocations.add(batch);
      return batch;
    };
    VectorizedRowBatch initial = factory.get();
    assertEquals(context, sequence.rowCapacity, initial.getMaxSize());
    List<Long> expectedRowIds = new ArrayList<>();
    for (GeneratedFrame frame : sequence.frames) {
      for (NullableValue<?> row : frame.columns.get(0)) {
        assertFalse(context, row.isNull);
        expectedRowIds.add((Long) row.value);
      }
    }
    assertEquals(context + ", distinct row IDs", expectedRowIds.size(),
        new HashSet<>(expectedRowIds).size());
    when(batchContext.createVectorizedRowBatch()).thenAnswer(invocation -> factory.get());
    when(reducer.batchNeedsClone()).thenReturn(retained);
    List<Integer> emittedSizes = new ArrayList<>();
    List<Long> emittedRowIds = new ArrayList<>();
    List<VectorizedRowBatch> retainedOutputs = new ArrayList<>();
    int[] processingFrame = {-1};
    doAnswer(invocation -> {
      int batchIndex = emittedSizes.size();
      assertTrue(context + ", unexpected emission at frame=" + processingFrame[0],
          batchIndex < sequence.expectedBatches.size());
      ExpectedSequenceBatch expected = sequence.expectedBatches.get(batchIndex);
      String batchContextMessage = context + ", emitted batch=" + batchIndex
          + ", frame=" + processingFrame[0] + ", reason=" + expected.reason;
      assertEquals(batchContextMessage, expected.flushFrame(sequence.frames.size()),
          processingFrame[0]);
      VectorizedRowBatch emitted = invocation.getArgument(0);
      // Inspect all values before a reusable batch can be reset by the receiver.
      assertCompatibleFrameSequenceResult(sequence, emitted, expected.firstFrame,
          expected.endFrame, batchContextMessage);
      emittedSizes.add(emitted.size);
      LongColumnVector rowIds = (LongColumnVector) emitted.cols[0];
      for (int row = 0; row < emitted.size; row++) {
        emittedRowIds.add(rowIds.vector[row]);
      }
      if (retained) {
        retainedOutputs.add(emitted);
      }
      return null;
    }).when(reducer).process(any(), anyInt());
    VectorShuffleBatchAccumulator accumulator = new VectorShuffleBatchAccumulator(
        batchContext, reducer, initial, sequence.adapters.length, (byte) 0,
        () -> {
          throw new AssertionError(context + ": compact inputs must not create row readers");
        });

    for (int frameIndex = 0; frameIndex < sequence.frames.size(); frameIndex++) {
      processingFrame[0] = frameIndex;
      GeneratedFrame frame = sequence.frames.get(frameIndex);
      String frameContext = context + ", frame=" + frameIndex + ", rows=" + frame.source.size
          + ", selected=" + frame.source.selectedInUse;
      int length = serializeSequenceFrame(frame, buffer);
      assertEquals(frameContext, lengths[frameIndex], length);
      BytesWritable encoded = new BytesWritable();
      encoded.setDirect(buffer, SEQUENCE_FRAME_OFFSET, length);
      assertAppendableSequenceFrame(frame.source, encoded, frameContext);
      accumulator.process(EMPTY, encoded, true);
      // The next frame borrows the same array; neither pending nor retained output may alias it.
      Arrays.fill(buffer, SEQUENCE_FRAME_OFFSET, SEQUENCE_FRAME_OFFSET + length, (byte) 0xff);

      int expectedEmissions = 0;
      int pendingStart = 0;
      for (ExpectedSequenceBatch expected : sequence.expectedBatches) {
        if (expected.flushFrame(sequence.frames.size()) <= frameIndex) {
          expectedEmissions++;
          pendingStart = expected.endFrame;
        }
      }
      assertEquals(frameContext + ", emissions after process", expectedEmissions, emittedSizes.size());
      assertEquals(frameContext + ", allocations", retained ? expectedEmissions + 1 : 1,
          allocations.size());
      VectorizedRowBatch pending = allocations.get(allocations.size() - 1);
      if (pendingStart <= frameIndex) {
        // Recheck earlier frames after every append, including nested offsets and metadata.
        assertCompatibleFrameSequenceResult(sequence, pending, pendingStart,
            frameIndex + 1, frameContext + ", pending");
      } else {
        assertEquals(frameContext + ", empty after automatic flush", 0, pending.size);
      }
    }

    processingFrame[0] = sequence.frames.size();
    accumulator.flushPendingBatch();
    assertEquals(context + ", all planned batches emitted", sequence.expectedBatches.size(),
        emittedSizes.size());
    assertEquals(context + ", concatenated emitted rows", expectedRowIds, emittedRowIds);
    assertEquals(context + ", allocations after final flush", retained ? emittedSizes.size() + 1 : 1,
        allocations.size());
    assertEquals(context + ", empty after final flush", 0, allocations.get(allocations.size() - 1).size);
    for (int batch = 0; batch < retainedOutputs.size(); batch++) {
      ExpectedSequenceBatch expected = sequence.expectedBatches.get(batch);
      assertCompatibleFrameSequenceResult(sequence, retainedOutputs.get(batch),
          expected.firstFrame, expected.endFrame, context + ", retained batch=" + batch);
    }
    accumulator.flushPendingBatch();
    assertEquals(context + ", repeated final flush", sequence.expectedBatches.size(), emittedSizes.size());
  }

  private static void assertAppendableSequenceFrame(VectorizedRowBatch source, BytesWritable frame,
      String context) throws IOException {
    VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
    header.read(frame, source.numCols,
        new VectorShuffleBatchMetadata(source.cols, source.numCols).getByteCount());
    assertEquals(context, source.size, header.getRowCount());
    assertTrue(context, source.size > 1);
    assertFalse(context, header.hasNestedRepeating());
  }

  private static final int SEQUENCE_FRAME_OFFSET = 11;
  private static final BytesWritable EMPTY = new BytesWritable();
}
