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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/** Shared seeded batch generators and value assertions; decoder execution belongs to the tests. */
final class VectorShuffleBatchTestSupport {
  private VectorShuffleBatchTestSupport() {
  }

  static final long BIGINT_PROPERTY_TEST_SEED = 0x5EEDB16L;
  static final long STRING_PROPERTY_TEST_SEED = 0x5EED517L;
  static final long DECIMAL_PROPERTY_TEST_SEED = 0x5EEDDEC1L;
  static final long DOUBLE_PROPERTY_TEST_SEED = 0x5EEDD0B1L;
  static final long DATE_PROPERTY_TEST_SEED = 0x5EEDDA7EL;
  static final long TIMESTAMP_PROPERTY_TEST_SEED = 0x5EED715EL;
  static final long INTERVAL_DAY_TIME_PROPERTY_TEST_SEED = 0x5EED1D7L;
  static final long VOID_PROPERTY_TEST_SEED = 0x5EED701DL;
  static final long BYTES_SLICE_PROPERTY_TEST_SEED = 0x511CE517L;
  static final long COLUMN_MAPPING_PROPERTY_TEST_SEED = 0xC01A4A9L;
  static final long ARRAY_BIGINT_PROPERTY_TEST_SEED = 0xA22A7B16L;
  static final long ARRAY_STRING_PROPERTY_TEST_SEED = 0xA22A7517L;
  static final long ARRAY_DECIMAL_PROPERTY_TEST_SEED = 0xA22ADEC1L;
  static final long NESTED_ARRAY_PROPERTY_TEST_SEED = 0xA22AA22AL;
  static final long NESTED_REPEATING_PROPERTY_TEST_SEED = 0xAE57ED5EEDL;
  static final long STRUCT_PROPERTY_TEST_SEED = 0x572AC7L;
  static final long NESTED_STRUCT_PROPERTY_TEST_SEED = 0x572AC7A22AL;
  static final long UNION_PROPERTY_TEST_SEED = 0xA110L;
  static final long NESTED_UNION_PROPERTY_TEST_SEED = 0xA110A22AL;
  static final long STRUCT_WITH_UNION_PROPERTY_TEST_SEED = 0x572A110L;
  static final long UNION_WITH_STRUCT_PROPERTY_TEST_SEED = 0xA110572L;
  static final long ARRAY_STRUCT_PROPERTY_TEST_SEED = 0xA22572L;
  static final long ARRAY_UNION_PROPERTY_TEST_SEED = 0xA22A110L;
  static final long MAP_PROPERTY_TEST_SEED = 0x6A4B16L;
  static final long NESTED_MAP_PROPERTY_TEST_SEED = 0x6A4A22AL;
  static final long COMPLEX_MAP_PROPERTY_TEST_SEED = 0x6A4572A110L;
  static final long MAP_MAP_PROPERTY_TEST_SEED = 0x6A46A4L;
  static final long ARRAY_MAP_PROPERTY_TEST_SEED = 0xA226A4L;
  static final long STRUCT_MAP_PROPERTY_TEST_SEED = 0x5726A4L;
  static final long UNION_MAP_PROPERTY_TEST_SEED = 0xA1106A4L;
  static final int BIGINT_PROPERTY_TEST_ITERATIONS = 10000;
  static final int PROPERTY_TEST_ITERATIONS = 2000;
  static final int BYTES_SLICE_PROPERTY_TEST_ITERATIONS = 200;
  static final int COLUMN_MAPPING_PROPERTY_TEST_ITERATIONS = 200;
  static final int ARRAY_PROPERTY_TEST_ITERATIONS = 1000;
  static final int NESTED_ARRAY_PROPERTY_TEST_ITERATIONS = 500;
  static final int COMPLEX_PROPERTY_TEST_ITERATIONS = 500;
  static final int NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS = 200;
  static final int MAX_STRING_LENGTH = 48;
  static final int MAX_ARRAY_LENGTH = 24;
  static final int MAX_MAP_LENGTH = 24;
  static final int[] INTERESTING_LENGTHS = {0, 1, 127, 128, 255, 256};
  static final int LENGTH_BOUNDARY_FIRST_ITERATION = 6;
  static final int LENGTH_BOUNDARY_LAYOUT_COUNT = 4;
  static final int SERIALIZE_BUFFER_SIZE = 32 * 1024 * 1024;
  static final int DECIMAL_PRECISION = 20;
  static final int DECIMAL64_PRECISION = 18;
  static final int DECIMAL_SCALE = 4;
  static final int MAX_LOGGED_PROPERTY_TEST_CASES = 10;
  static final byte[] ALPHANUMERIC =
      bytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");

  static final long[] INTERESTING_BIGINTS = {
      Long.MIN_VALUE, Long.MIN_VALUE + 1, Integer.MIN_VALUE, -11, -10, -1, 0, 1, 10, 11,
      Integer.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE
  };

  static final byte[][] INTERESTING_STRINGS = {
      bytes(""), bytes("a"), bytes("alpha"), bytes("with spaces"),
      bytes("special-_.:/@#"), bytes("quote\"slash\\")
  };

  static final double[] INTERESTING_DOUBLES = {
      Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -Double.MIN_NORMAL, -Double.MIN_VALUE, -0.0d,
      0.0d, Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE, Double.POSITIVE_INFINITY,
      Double.NaN
  };

  static final HiveDecimal[] INTERESTING_DECIMALS = {
      HiveDecimal.create("1234567890.1234"),
      HiveDecimal.create("-9999999999999999.9999"), HiveDecimal.create("-1.0000"),
      HiveDecimal.create("-0.0001"), HiveDecimal.ZERO, HiveDecimal.create("0.0001"),
      HiveDecimal.create("1.0000"), HiveDecimal.create("9999999999999999.9999")
  };

  static final HiveDecimal[] INTERESTING_DECIMAL64S = {
      HiveDecimal.create("1234567890.1234"),
      HiveDecimal.create("-99999999999999.9999"), HiveDecimal.create("-1.0000"),
      HiveDecimal.create("-0.0001"), HiveDecimal.ZERO, HiveDecimal.create("0.0001"),
      HiveDecimal.create("1.0000"), HiveDecimal.create("99999999999999.9999")
  };

  static List<byte[]> randomSliceValues(Random random, int size, int iteration) {
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

  static void applySliceValues(BytesColumnVector source, RandomBatchLayout layout,
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

  static void applySharedSliceValues(BytesColumnVector source, RandomBatchLayout layout,
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

  static int forcedPrefixLength(int iteration, Random random) {
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

  static byte[] sliceBackingArray(byte[] value, int prefixLength, int suffixLength,
      int logical) {
    byte[] backing = new byte[prefixLength + value.length + suffixLength];
    Arrays.fill(backing, (byte) ('A' + logical % 26));
    System.arraycopy(value, 0, backing, prefixLength, value.length);
    return backing;
  }

  static void assertBytesColumnEquals(List<byte[]> expected, BytesColumnVector actual,
      String context) {
    for (int logical = 0; logical < expected.size(); logical++) {
      assertBytesValueEquals(context + ", logicalRow=" + logical, expected.get(logical), actual,
          logical);
    }
  }

  static int randomMappingSourceColumnCount(Random random, int iteration) {
    if (iteration < 6) {
      return iteration == 5 ? 5 : 4;
    }
    return 1 + random.nextInt(8);
  }

  static int[] randomSourceColumnMap(Random random, int iteration, int sourceColumnCount) {
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
    case 6:
    case 7:
      return new int[0];
    default:
      int length = random.nextInt(sourceColumnCount * 2 + 1);
      int[] sourceColumnMap = new int[length];
      for (int index = 0; index < sourceColumnMap.length; index++) {
        sourceColumnMap[index] = random.nextInt(sourceColumnCount);
      }
      return sourceColumnMap;
    }
  }

  static int[] identityMap(int length) {
    int[] map = new int[length];
    for (int index = 0; index < map.length; index++) {
      map[index] = index;
    }
    return map;
  }

  static int[] reverseMap(int length) {
    int[] map = new int[length];
    for (int index = 0; index < map.length; index++) {
      map[index] = length - index - 1;
    }
    return map;
  }

  static VectorizedRowBatch randomMappingSourceBatch(RandomBatchLayout layout,
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

  static VectorizedRowBatch mappingDestinationBatch(int[] sourceColumnMap) {
    VectorizedRowBatch destination = new VectorizedRowBatch(sourceColumnMap.length);
    for (int column = 0; column < sourceColumnMap.length; column++) {
      destination.cols[column] = mappingColumnVector(sourceColumnMap[column]);
    }
    // Constructors set size to capacity; accumulation must start with an empty batch.
    destination.reset();
    return destination;
  }

  static ColumnVector mappingColumnVector(int sourceColumn) {
    return sourceColumn % 2 == 0 ? new LongColumnVector() : new DoubleColumnVector();
  }

  static void setMappingValue(ColumnVector vector, int sourceColumn, int row) {
    if (vector instanceof LongColumnVector) {
      ((LongColumnVector) vector).vector[row] = expectedMappingLongValue(sourceColumn, row);
    } else {
      ((DoubleColumnVector) vector).vector[row] = expectedMappingDoubleValue(sourceColumn, row);
    }
  }

  static void assertColumnMappingRoundTrip(RandomBatchLayout layout, int[] sourceColumnMap,
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

  static void assertMappingValue(String context, int sourceColumn, int sourceRow,
      ColumnVector vector, int destinationRow) {
    if (sourceColumn % 2 == 0) {
      assertEquals(context, expectedMappingLongValue(sourceColumn, sourceRow),
          ((LongColumnVector) vector).vector[destinationRow]);
    } else {
      assertEquals(context, expectedMappingDoubleValue(sourceColumn, sourceRow),
          ((DoubleColumnVector) vector).vector[destinationRow], 0.0);
    }
  }

  static long expectedMappingLongValue(int sourceColumn, int row) {
    return ((long) sourceColumn << 32) | row;
  }

  static double expectedMappingDoubleValue(int sourceColumn, int row) {
    return sourceColumn * 1000000.0d + row + 0.25d;
  }

  static RandomBatchLayout randomBatchLayout(Random random, int iteration) {
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

  static RandomColumnLayout randomColumnLayout(Random random, int size, int iteration) {
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

  static int[] shuffledIndices(Random random) {
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

  static RandomColumnAdapter<?> nestedRepeatingAdapter(NestedRepeatingContainer container,
      RandomColumnAdapter<?> child) {
    switch (container) {
    case ARRAY:
      return arrayAdapter(child);
    case MAP:
      return mapAdapter(bigIntAdapter(), child);
    case STRUCT:
      return structAdapter(child);
    case UNION:
      return unionAdapter(child, bigIntAdapter());
    default:
      throw new AssertionError(container);
    }
  }

  static NestedRepeatingValue nestedRepeatingPayload(Random random, int iteration,
      int depth) {
    RandomColumnAdapter<?> leaf = iteration / 16 % 3 == 0 ? bigIntAdapter()
        : iteration / 16 % 3 == 1 ? stringAdapter() : decimalAdapter();
    NestedRepeatingValue payload = new NestedRepeatingValue(leaf,
        randomAdapterValue(leaf, random, null, iteration + 8));
    for (int level = 0; level < depth; level++) {
      NestedRepeatingContainer container =
          NestedRepeatingContainer.values()[(iteration / 4 + level) % 4];
      NullableValue<Object> child = new NullableValue<>(false, payload.value);
      Object value;
      switch (container) {
      case ARRAY:
        value = new ArrayValue<>(0, Arrays.asList(child));
        break;
      case MAP:
        value = new MapValue<>(0,
            Arrays.asList(new MapEntryValue<>(random.nextLong(), child)));
        break;
      case STRUCT:
        value = new StructValue(Arrays.<NullableValue<?>>asList(child));
        break;
      case UNION:
        value = new UnionValue(0, child);
        break;
      default:
        throw new AssertionError(container);
      }
      payload = new NestedRepeatingValue(nestedRepeatingAdapter(container, payload.adapter), value);
    }
    return payload;
  }

  static void setRepeatingRecursively(ColumnVector vector) {
    vector.isRepeating = true;
    if (vector instanceof ListColumnVector) {
      setRepeatingRecursively(((ListColumnVector) vector).child);
    } else if (vector instanceof MapColumnVector) {
      setRepeatingRecursively(((MapColumnVector) vector).keys);
      setRepeatingRecursively(((MapColumnVector) vector).values);
    } else if (vector instanceof StructColumnVector || vector instanceof UnionColumnVector) {
      ColumnVector[] fields = vector instanceof StructColumnVector
          ? ((StructColumnVector) vector).fields : ((UnionColumnVector) vector).fields;
      for (ColumnVector field : fields) {
        setRepeatingRecursively(field);
      }
    }
  }

  static NestedRepeatingCase nestedRepeatingCase(Random random,
      NestedRepeatingContainer container, NestedRepeatingValue payload, int children, int rows,
      boolean repeating, boolean selected, boolean childNull) {
    ColumnVector child = payload.adapter.createVector();
    setAdapterValue(payload.adapter, child, 0, payload.value);
    // Every payload collection has one element and starts at zero, so all representatives
    // are initialized before changing the physical representation recursively.
    setRepeatingRecursively(child);
    if (childNull) {
      setNull(child, 0);
    }
    NullableValue<Object> expectedChild = new NullableValue<>(childNull,
        childNull ? null : payload.value);
    ColumnVector root;
    switch (container) {
    case ARRAY:
      root = new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, child);
      break;
    case MAP:
      root = new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, new LongColumnVector(), child);
      break;
    case STRUCT:
      root = structOf(child);
      break;
    case UNION:
      root = unionOf(child, new LongColumnVector());
      break;
    default:
      throw new AssertionError(container);
    }
    root.isRepeating = repeating;
    RandomBatchLayout layout = new RandomBatchLayout(rows, selected ? shuffledIndices(random) : null);
    List<NullableValue<?>> expected = new ArrayList<>();
    for (int logical = 0; logical < (repeating ? 1 : rows); logical++) {
      int physical = repeating ? 0 : layout.sourceIndex(logical);
      Object value;
      if (root instanceof MultiValuedColumnVector) {
        MultiValuedColumnVector multi = (MultiValuedColumnVector) root;
        int length = logical == 0 ? children : 0;
        int offset = multi.childCount + random.nextInt(4);
        multi.offsets[physical] = offset;
        multi.lengths[physical] = length;
        multi.childCount = offset + length;
        if (root instanceof ListColumnVector) {
          List<NullableValue<Object>> elements = new ArrayList<>();
          for (int element = 0; element < length; element++) {
            elements.add(expectedChild);
          }
          value = new ArrayValue<>(0, elements);
        } else {
          List<MapEntryValue<Long, Object>> entries = new ArrayList<>();
          for (int entry = 0; entry < length; entry++) {
            long key = random.nextLong();
            ((LongColumnVector) ((MapColumnVector) root).keys).vector[offset + entry] = key;
            entries.add(new MapEntryValue<>(key, expectedChild));
          }
          value = new MapValue<>(0, entries);
        }
      } else if (root instanceof StructColumnVector) {
        if (logical >= children) {
          setNull(root, physical);
          expected.add(new NullableValue<>(true, null));
          continue;
        }
        value = new StructValue(Arrays.<NullableValue<?>>asList(expectedChild));
      } else {
        UnionColumnVector union = (UnionColumnVector) root;
        int tag = logical < children ? 0 : 1;
        union.tags[physical] = tag;
        long ordinary = random.nextLong();
        ((LongColumnVector) union.fields[1]).vector[physical] = ordinary;
        value = new UnionValue(tag, tag == 0 ? expectedChild
            : new NullableValue<>(false, ordinary));
      }
      expected.add(new NullableValue<>(false, value));
    }
    while (expected.size() < rows) {
      expected.add(expected.get(0));
    }
    return new NestedRepeatingCase(layout.batchWithColumn(root),
        nestedRepeatingAdapter(container, payload.adapter), expected);
  }

  static NestedRepeatingCase encloseNestedRepeatingCase(NestedRepeatingCase scenario,
      int depth) {
    for (int level = 0; level < depth; level++) {
      List<NullableValue<?>> expected = new ArrayList<>();
      for (NullableValue<?> value : scenario.expected) {
        expected.add(new NullableValue<>(false,
            new StructValue(Arrays.<NullableValue<?>>asList(value))));
      }
      VectorizedRowBatch source = batchWithColumn(structOf(scenario.source.cols[0]),
          scenario.source.size);
      source.selectedInUse = scenario.source.selectedInUse;
      System.arraycopy(scenario.source.selected, 0, source.selected, 0, source.size);
      scenario = new NestedRepeatingCase(source, structAdapter(scenario.adapter), expected);
    }
    return scenario;
  }

  static VectorizedRowBatch nestedRepeatingDestination(NestedRepeatingCase[] scenarios) {
    ColumnVector[] columns = new ColumnVector[scenarios.length];
    for (int column = 0; column < columns.length; column++) {
      columns[column] = scenarios[column].adapter.createDestinationVector();
    }
    return batch(0, columns);
  }

  static void assertNestedRepeatingBatch(NestedRepeatingCase[] scenarios,
      VectorizedRowBatch actual, int copies, boolean standalone, String context) {
    assertEquals(context, scenarios[0].source.size * copies, actual.size);
    assertFalse(context, actual.selectedInUse);
    assertEquals(context, scenarios.length, actual.projectionSize);
    for (int column = 0; column < scenarios.length; column++) {
      assertEquals(context, column, actual.projectedColumns[column]);
      assertNestedRepeatingColumn(scenarios[column], actual.cols[column], actual.size,
          standalone || scenarios[column].source.cols[0].isRepeating,
          context + ", column=" + column);
    }
  }

  static void assertNestedRepeatingColumn(NestedRepeatingCase scenario, ColumnVector actual,
      int rows, boolean preserveRepeating, String context) {
    List<NullableValue<?>> values = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();
    for (int row = 0; row < rows; row++) {
      NullableValue<?> expected = scenario.expected.get(row % scenario.source.size);
      values.add(expected);
      positions.add(row);
      int index = actual.isRepeating ? 0 : row;
      assertEquals(context + ", row=" + row, expected.isNull, isNull(actual, index));
      if (!expected.isNull) {
        assertAdapterValue(scenario.adapter, context + ", row=" + row, expected.value,
            actual, index);
      }
    }
    assertNestedRepeatingLayout(context, scenario.source.cols[0], actual, values,
        positions, preserveRepeating);
  }

  static void assertNestedRepeatingLayout(String context, ColumnVector source,
      ColumnVector actual, List<NullableValue<?>> values, List<Integer> positions,
      boolean preserveRepeating) {
    assertEquals(context + ", repeating", preserveRepeating && source.isRepeating,
        actual.isRepeating);
    int count = actual.isRepeating ? Math.min(values.size(), 1) : values.size();
    if (actual instanceof MultiValuedColumnVector) {
      MultiValuedColumnVector multi = (MultiValuedColumnVector) actual;
      List<NullableValue<?>> children = new ArrayList<>();
      List<NullableValue<?>> keys = new ArrayList<>();
      List<Integer> childPositions = new ArrayList<>();
      for (int logical = 0; logical < count; logical++) {
        int index = actual.isRepeating ? 0 : positions.get(logical);
        NullableValue<?> value = values.get(logical);
        int start = children.size();
        assertEquals(context + ", offset=" + index, start, multi.offsets[index]);
        if (!value.isNull) {
          if (actual instanceof ListColumnVector) {
            children.addAll(((ArrayValue<?>) value.value).elements);
          } else {
            for (MapEntryValue<?, ?> entry : ((MapValue<?, ?>) value.value).entries) {
              keys.add(new NullableValue<>(false, entry.key));
              children.add(entry.value);
            }
          }
        }
        assertEquals(context + ", length=" + index, children.size() - start, multi.lengths[index]);
      }
      assertEquals(context + ", childCount", children.size(), multi.childCount);
      for (int child = 0; child < children.size(); child++) {
        childPositions.add(child);
      }
      if (actual instanceof ListColumnVector) {
        assertNestedRepeatingLayout(context + "/element", ((ListColumnVector) source).child,
            ((ListColumnVector) actual).child, children, childPositions, preserveRepeating);
      } else {
        assertNestedRepeatingLayout(context + "/key", ((MapColumnVector) source).keys,
            ((MapColumnVector) actual).keys, keys, childPositions, preserveRepeating);
        assertNestedRepeatingLayout(context + "/value", ((MapColumnVector) source).values,
            ((MapColumnVector) actual).values, children, childPositions, preserveRepeating);
      }
    } else if (actual instanceof StructColumnVector || actual instanceof UnionColumnVector) {
      ColumnVector[] sourceFields = actual instanceof StructColumnVector
          ? ((StructColumnVector) source).fields : ((UnionColumnVector) source).fields;
      ColumnVector[] actualFields = actual instanceof StructColumnVector
          ? ((StructColumnVector) actual).fields : ((UnionColumnVector) actual).fields;
      for (int field = 0; field < actualFields.length; field++) {
        List<NullableValue<?>> children = new ArrayList<>();
        List<Integer> childPositions = new ArrayList<>();
        for (int logical = 0; logical < count; logical++) {
          NullableValue<?> value = values.get(logical);
          if (value.isNull) {
            continue;
          }
          if (actual instanceof StructColumnVector) {
            children.add(((StructValue) value.value).fields.get(field));
          } else {
            UnionValue union = (UnionValue) value.value;
            if (union.tag != field) {
              continue;
            }
            children.add(union.value);
          }
          childPositions.add(actual.isRepeating ? 0 : positions.get(logical));
        }
        assertNestedRepeatingLayout(context + "/field=" + field, sourceFields[field],
            actualFields[field], children, childPositions, preserveRepeating);
      }
    }
  }

  static RandomColumnScenario randomColumnScenario(Random random, int iteration) {
    RandomBatchLayout batchLayout = randomBatchLayout(random, iteration);
    return new RandomColumnScenario(batchLayout,
        randomColumnLayout(random, batchLayout.size, iteration));
  }

  static <T> List<T> randomValues(Random random, RandomColumnScenario scenario,
      RandomColumnAdapter<T> adapter, int iteration) {
    List<T> values = new ArrayList<>(scenario.size());
    T previous = null;
    for (int logical = 0; logical < scenario.size(); logical++) {
      T value = scenario.columnLayout.isRepeating && logical > 0
          ? values.get(0) : adapter.randomValueForScenario(random, previous, logical, iteration);
      values.add(value);
      previous = value;
    }
    return values;
  }

  static <T> void assertColumnRoundTrip(List<T> expected, RandomColumnScenario scenario,
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

  static RandomColumnAdapter<Long> bigIntAdapter() {
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

  static RandomColumnAdapter<byte[]> stringAdapter() {
    return new VariableLengthColumnAdapter<byte[]>() {
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
      public byte[] randomValueOfLength(Random random, int length) {
        return randomStringOfLength(random, length);
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

  static RandomColumnAdapter<HiveDecimal> decimalAdapter() {
    return decimalAdapter(false, false);
  }

  static RandomColumnAdapter<HiveDecimal> decimalAdapter(boolean sourceDecimal64,
      boolean destinationDecimal64) {
    // Decimal64 requires at most 18 digits; retain the wider ordinary-decimal coverage.
    int precision = sourceDecimal64 || destinationDecimal64
        ? DECIMAL64_PRECISION : DECIMAL_PRECISION;
    HiveDecimal[] interesting = sourceDecimal64 || destinationDecimal64
        ? INTERESTING_DECIMAL64S : INTERESTING_DECIMALS;
    return new RandomColumnAdapter<HiveDecimal>() {
      @Override
      public ColumnVector createVector() {
        return sourceDecimal64 ? new Decimal64ColumnVector(precision, DECIMAL_SCALE)
            : new DecimalColumnVector(precision, DECIMAL_SCALE);
      }

      @Override
      public ColumnVector createDestinationVector() {
        return destinationDecimal64 ? new Decimal64ColumnVector(precision, DECIMAL_SCALE)
            : new DecimalColumnVector(precision, DECIMAL_SCALE);
      }

      @Override
      public HiveDecimal randomValue(Random random, HiveDecimal previous, int logical) {
        if (logical < interesting.length) {
          return interesting[logical];
        }
        return randomDecimal(random, previous, interesting, precision);
      }

      @Override
      public void setValue(ColumnVector vector, int index, HiveDecimal value) {
        if (sourceDecimal64) {
          ((Decimal64ColumnVector) vector).set(index, value);
        } else {
          ((DecimalColumnVector) vector).set(index, value);
        }
      }

      @Override
      public void assertValueEquals(String context, HiveDecimal expected, ColumnVector vector,
          int index) {
        assertEquals(context, destinationDecimal64, vector instanceof Decimal64ColumnVector);
        if (destinationDecimal64) {
          Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) vector;
          assertEquals(context, new HiveDecimalWritable(expected).serialize64(DECIMAL_SCALE),
              decimal64.vector[index]);
          HiveDecimalWritable actual = new HiveDecimalWritable();
          actual.deserialize64(decimal64.vector[index], decimal64.scale);
          assertEquals(context, expected, actual.getHiveDecimal());
        } else {
          assertEquals(context, expected,
              ((DecimalColumnVector) vector).vector[index].getHiveDecimal());
        }
      }

      @Override
      public void assertColumnInvariants(String context, List<HiveDecimal> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        assertEquals(context, destinationDecimal64, vector instanceof Decimal64ColumnVector);
        if (destinationDecimal64) {
          Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) vector;
          assertEquals(context, precision, decimal64.precision);
          assertEquals(context, DECIMAL_SCALE, decimal64.scale);
        } else {
          DecimalColumnVector decimal = (DecimalColumnVector) vector;
          assertEquals(context, precision, decimal.precision);
          assertEquals(context, DECIMAL_SCALE, decimal.scale);
        }
      }

      @Override
      public String formatValue(HiveDecimal value) {
        return value.toString();
      }
    };
  }

  static RandomColumnAdapter<Void> voidAdapter() {
    return new RandomColumnAdapter<Void>() {
      @Override
      public ColumnVector createVector() {
        return new VoidColumnVector();
      }

      @Override
      public RandomColumnScenario randomScenario(Random random, int iteration) {
        RandomBatchLayout layout = randomBatchLayout(random, iteration);
        if (iteration == 5) {
          layout = new RandomBatchLayout(17, shuffledIndices(random));
        }
        // VOID has no non-NULL values. Pin repeating boundary cases, then vary the layout.
        boolean repeating = iteration <= 5 || random.nextBoolean();
        return new RandomColumnScenario(layout,
            RandomColumnLayout.allNull(layout.size, repeating));
      }

      @Override
      public Void randomValue(Random random, Void previous, int logical) {
        return null;
      }

      @Override
      public void setValue(ColumnVector vector, int index, Void value) {
        throw new AssertionError("VOID cannot contain a non-NULL value");
      }

      @Override
      public void assertValueEquals(String context, Void expected, ColumnVector vector,
          int index) {
        throw new AssertionError(context + ": VOID cannot contain a non-NULL value");
      }

      @Override
      public void assertColumnInvariants(String context, List<Void> expected,
          RandomColumnScenario scenario, ColumnVector vector) {
        assertTrue(context, vector instanceof VoidColumnVector);
        assertEquals(context, scenario.size() == 0, vector.noNulls);
        assertEquals(context, scenario.size() > 0, vector.isNull[0]);
      }

      @Override
      public String formatValue(Void value) {
        return "NULL";
      }
    };
  }

  static RandomColumnAdapter<Double> doubleAdapter() {
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

  static RandomColumnAdapter<Long> dateAdapter(boolean proleptic) {
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

  static RandomColumnAdapter<TimestampValue> timestampAdapter(boolean isUtc,
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

  static RandomColumnAdapter<IntervalDayTimeValue> intervalDayTimeAdapter() {
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

  static <T> RandomColumnAdapter<ArrayValue<T>> arrayAdapter(
      RandomColumnAdapter<T> childAdapter) {
    return new VariableLengthColumnAdapter<ArrayValue<T>>() {
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
        return randomArrayOfLength(random, length, true);
      }

      @Override
      public ArrayValue<T> randomValueOfLength(Random random, int length) {
        // Preserve dense child payloads at every length boundary as well as nullable random cases.
        return randomArrayOfLength(random, length, false);
      }

      private ArrayValue<T> randomArrayOfLength(Random random, int length, boolean allowNulls) {
        List<NullableValue<T>> elements = new ArrayList<>(length);
        T previousElement = null;
        for (int element = 0; element < length; element++) {
          boolean isNull = allowNulls && random.nextInt(5) == 0;
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
          int childIndex = list.child.isRepeating ? 0 : offset + element;
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

  static <K, V> RandomColumnAdapter<MapValue<K, V>> mapAdapter(
      RandomColumnAdapter<K> keyAdapter, RandomColumnAdapter<V> valueAdapter) {
    return new VariableLengthColumnAdapter<MapValue<K, V>>() {
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
        return randomValueOfLength(random, length);
      }

      @Override
      public MapValue<K, V> randomValueOfLength(Random random, int length) {
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
          int keyIndex = map.keys.isRepeating ? 0 : childIndex;
          int valueIndex = map.values.isRepeating ? 0 : childIndex;
          String entryContext = context + ", entry=" + entry;
          assertFalse(entryContext + ", key", isNull(map.keys, keyIndex));
          keyAdapter.assertValueEquals(entryContext + ", key", expectedEntry.key, map.keys,
              keyIndex);
          boolean actualValueIsNull = isNull(map.values, valueIndex);
          assertEquals(entryContext + ", value", expectedEntry.value.isNull, actualValueIsNull);
          if (!actualValueIsNull) {
            valueAdapter.assertValueEquals(entryContext + ", value", expectedEntry.value.value,
                map.values, valueIndex);
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

  static RandomColumnAdapter<StructValue> structAdapter(
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

  static RandomColumnAdapter<UnionValue> unionAdapter(
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
  static Object randomAdapterValue(RandomColumnAdapter<?> adapter, Random random,
      Object previous, int logical) {
    return ((RandomColumnAdapter<Object>) adapter).randomValue(random, previous, logical);
  }

  @SuppressWarnings("unchecked")
  static void setAdapterValue(RandomColumnAdapter<?> adapter, ColumnVector vector,
      int index, Object value) {
    ((RandomColumnAdapter<Object>) adapter).setValue(vector, index, value);
  }

  @SuppressWarnings("unchecked")
  static void assertAdapterValue(RandomColumnAdapter<?> adapter, String context,
      Object expected, ColumnVector vector, int index) {
    ((RandomColumnAdapter<Object>) adapter).assertValueEquals(context, expected, vector, index);
  }

  @SuppressWarnings("unchecked")
  static String formatAdapterValue(RandomColumnAdapter<?> adapter, Object value) {
    return ((RandomColumnAdapter<Object>) adapter).formatValue(value);
  }

  static void setNull(ColumnVector vector, int index) {
    vector.noNulls = false;
    vector.isNull[index] = true;
  }

  static boolean isNull(ColumnVector vector, int index) {
    return !vector.noNulls && vector.isNull[index];
  }

  static long randomBigInt(Random random, Long previous) {
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

  static String formatPrintableAsciiString(byte[] value) {
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

  static byte[] randomString(Random random, byte[] previous) {
    int choice = random.nextInt(100);
    if (choice < 20) {
      return INTERESTING_STRINGS[random.nextInt(INTERESTING_STRINGS.length)];
    } else if (choice >= 90 && previous != null) {
      return previous;
    }
    return randomStringOfLength(random, random.nextInt(MAX_STRING_LENGTH + 1));
  }

  static byte[] randomStringOfLength(Random random, int length) {
    byte[] value = new byte[length];
    for (int index = 0; index < length; index++) {
      value[index] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
    }
    return value;
  }

  static HiveDecimal randomDecimal(Random random, HiveDecimal previous,
      HiveDecimal[] interesting, int precision) {
    int choice = random.nextInt(100);
    if (choice < 20) {
      return interesting[random.nextInt(interesting.length)];
    } else if (choice >= 90 && previous != null) {
      return previous;
    }
    BigInteger unscaled = choice < 60
        ? BigInteger.valueOf(random.nextInt(200001) - 100000)
        : new BigInteger(66, random).mod(BigInteger.TEN.pow(precision))
            .multiply(random.nextBoolean() ? BigInteger.ONE : BigInteger.valueOf(-1));
    return HiveDecimal.create(unscaled, DECIMAL_SCALE);
  }

  static <T> RandomColumnLog<T> newColumnLog(String logPrefix, String typeName, long seed,
      RandomColumnAdapter<T> adapter) throws IOException {
    Path directory = Paths.get(System.getProperty("test.tmp.dir", "target/tmp"));
    Files.createDirectories(directory);
    String fileName = logPrefix + typeName.replaceAll("[^A-Za-z0-9]+", "-") + ".log";
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

  interface RandomColumnLog<T> extends AutoCloseable {
    void write(int iteration, RandomColumnScenario scenario, List<T> values) throws IOException;

    @Override
    void close() throws IOException;
  }

  interface RandomColumnAdapter<T> {
    ColumnVector createVector();

    default ColumnVector createDestinationVector() {
      return createVector();
    }

    default RandomColumnScenario randomScenario(Random random, int iteration) {
      return randomColumnScenario(random, iteration);
    }

    T randomValue(Random random, T previous, int logical);

    default T randomValueForScenario(Random random, T previous, int logical, int iteration) {
      return randomValue(random, previous, logical);
    }

    void setValue(ColumnVector vector, int index, T value);

    void assertValueEquals(String context, T expected, ColumnVector vector, int index);

    String formatValue(T value);

    default void assertColumnInvariants(String context, List<T> expected,
        RandomColumnScenario scenario, ColumnVector vector) {
    }
  }

  /** Large length cases apply only to the root vector, keeping recursive values bounded. */
  interface VariableLengthColumnAdapter<T> extends RandomColumnAdapter<T> {
    T randomValueOfLength(Random random, int length);

    @Override
    default RandomColumnScenario randomScenario(Random random, int iteration) {
      int boundaryIteration = iteration - LENGTH_BOUNDARY_FIRST_ITERATION;
      if (boundaryIteration < 0
          || boundaryIteration >= INTERESTING_LENGTHS.length * LENGTH_BOUNDARY_LAYOUT_COUNT) {
        return RandomColumnAdapter.super.randomScenario(random, iteration);
      }
      int mode = boundaryIteration % LENGTH_BOUNDARY_LAYOUT_COUNT;
      // Small batches bound child growth even when the root is a nested collection.
      // The fourth layout mixes every boundary length in a single selected batch.
      int size = mode == 3 ? INTERESTING_LENGTHS.length : 2 + random.nextInt(3);
      RandomBatchLayout layout = new RandomBatchLayout(size,
          mode == 1 || mode == 3 ? shuffledIndices(random) : null);
      return new RandomColumnScenario(layout, RandomColumnLayout.nonNull(size, mode == 2));
    }

    @Override
    default T randomValueForScenario(Random random, T previous, int logical, int iteration) {
      int boundaryIteration = iteration - LENGTH_BOUNDARY_FIRST_ITERATION;
      if (boundaryIteration < 0
          || boundaryIteration >= INTERESTING_LENGTHS.length * LENGTH_BOUNDARY_LAYOUT_COUNT) {
        return randomValue(random, previous, logical);
      }
      int lengthIndex = boundaryIteration / LENGTH_BOUNDARY_LAYOUT_COUNT;
      if (boundaryIteration % LENGTH_BOUNDARY_LAYOUT_COUNT == 3) {
        lengthIndex = (lengthIndex + logical) % INTERESTING_LENGTHS.length;
      }
      int length = INTERESTING_LENGTHS[lengthIndex];
      return randomValueOfLength(random, length);
    }
  }

  enum NestedRepeatingContainer {
    ARRAY, MAP, STRUCT, UNION
  }

  static final class NestedRepeatingValue {
    final RandomColumnAdapter<?> adapter;
    final Object value;

    NestedRepeatingValue(RandomColumnAdapter<?> adapter, Object value) {
      this.adapter = adapter;
      this.value = value;
    }
  }

  static final class NestedRepeatingCase {
    final VectorizedRowBatch source;
    final RandomColumnAdapter<?> adapter;
    final List<NullableValue<?>> expected;

    NestedRepeatingCase(VectorizedRowBatch source, RandomColumnAdapter<?> adapter,
        List<NullableValue<?>> expected) {
      this.source = source;
      this.adapter = adapter;
      this.expected = expected;
    }
  }

  static final class TimestampValue {
    final long time;
    final int nanos;

    TimestampValue(long time, int nanos) {
      this.time = time;
      this.nanos = nanos;
    }
  }

  static final class IntervalDayTimeValue {
    final long totalSeconds;
    final int nanos;

    IntervalDayTimeValue(long totalSeconds, int nanos) {
      this.totalSeconds = totalSeconds;
      this.nanos = nanos;
    }
  }

  static final class NullableValue<T> {
    final boolean isNull;
    final T value;

    NullableValue(boolean isNull, T value) {
      this.isNull = isNull;
      this.value = value;
    }
  }

  static final class MapEntryValue<K, V> {
    final K key;
    final NullableValue<V> value;

    MapEntryValue(K key, NullableValue<V> value) {
      this.key = key;
      this.value = value;
    }
  }

  static final class MapValue<K, V> {
    final int gapBefore;
    final List<MapEntryValue<K, V>> entries;

    MapValue(int gapBefore, List<MapEntryValue<K, V>> entries) {
      this.gapBefore = gapBefore;
      this.entries = entries;
    }
  }

  static final class StructValue {
    final List<NullableValue<?>> fields;

    StructValue(List<NullableValue<?>> fields) {
      this.fields = fields;
    }
  }

  static final class UnionValue {
    final int tag;
    final NullableValue<?> value;

    UnionValue(int tag, NullableValue<?> value) {
      this.tag = tag;
      this.value = value;
    }
  }

  static final class ArrayValue<T> {
    final int gapBefore;
    final List<NullableValue<T>> elements;

    ArrayValue(int gapBefore, List<NullableValue<T>> elements) {
      this.gapBefore = gapBefore;
      this.elements = elements;
    }
  }

  static final class RandomColumnScenario {
    final RandomBatchLayout batchLayout;
    final RandomColumnLayout columnLayout;

    RandomColumnScenario(RandomBatchLayout batchLayout, RandomColumnLayout columnLayout) {
      this.batchLayout = batchLayout;
      this.columnLayout = columnLayout;
    }

    int size() {
      return batchLayout.size;
    }

    int valueCount() {
      return columnLayout.isRepeating ? Math.min(size(), 1) : size();
    }

    boolean isNull(int logical) {
      return columnLayout.isNull(logical);
    }

    int sourceIndex(int logical) {
      return columnLayout.sourceIndex(batchLayout, logical);
    }

    @Override
    public String toString() {
      return batchLayout + ", " + columnLayout;
    }
  }

  /** A reusable description of logical rows and their physical positions in a source batch. */
  static final class RandomBatchLayout {
    final int size;
    final int[] selected;

    RandomBatchLayout(int size, int[] selected) {
      this.size = size;
      this.selected = selected;
    }

    int sourceIndex(int logical) {
      return selected == null ? logical : selected[logical];
    }

    VectorizedRowBatch batchWithColumn(ColumnVector column) {
      VectorizedRowBatch batch = VectorShuffleBatchTestSupport.batchWithColumn(column, size);
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
  static final class RandomColumnLayout {
    final boolean isRepeating;
    final boolean[] nulls;

    RandomColumnLayout(boolean isRepeating, boolean[] nulls) {
      this.isRepeating = isRepeating;
      this.nulls = nulls;
    }

    static RandomColumnLayout nonNull(int size, boolean repeating) {
      return new RandomColumnLayout(repeating, new boolean[repeating ? Math.min(size, 1) : size]);
    }

    static RandomColumnLayout allNull(int size, boolean repeating) {
      boolean[] nulls = new boolean[repeating ? Math.min(size, 1) : size];
      Arrays.fill(nulls, true);
      return new RandomColumnLayout(repeating, nulls);
    }

    static RandomColumnLayout randomNulls(Random random, int size, boolean repeating) {
      boolean[] nulls = new boolean[repeating ? Math.min(size, 1) : size];
      for (int logical = 0; logical < nulls.length; logical++) {
        nulls[logical] = random.nextInt(5) == 0;
      }
      return new RandomColumnLayout(repeating, nulls);
    }

    boolean isNull(int logical) {
      return nulls.length > 0 && nulls[isRepeating ? 0 : logical];
    }

    int sourceIndex(RandomBatchLayout batchLayout, int logical) {
      return isRepeating ? 0 : batchLayout.sourceIndex(logical);
    }

    void apply(ColumnVector column, RandomBatchLayout batchLayout) {
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

  static VectorizedRowBatch batch(int rows, ColumnVector... columns) {
    VectorizedRowBatch batch = new VectorizedRowBatch(columns.length);
    System.arraycopy(columns, 0, batch.cols, 0, columns.length);
    batch.size = rows;
    return batch;
  }

  static VectorizedRowBatch batchWithColumn(ColumnVector column, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = column;
    batch.size = size;
    return batch;
  }

  static StructColumnVector structOf(ColumnVector... fields) {
    return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  static UnionColumnVector unionOf(ColumnVector... fields) {
    return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, fields);
  }

  static void assertBytesValueEquals(String context, byte[] expected,
      BytesColumnVector actual, int index) {
    assertArrayEquals(context, expected, Arrays.copyOfRange(actual.vector[index],
        actual.start[index], actual.start[index] + actual.length[index]));
  }

  static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @FunctionalInterface
  interface CompatibleFrameSequenceConsumer {
    void accept(CompatibleFrameSequence sequence) throws Exception;
  }

  enum CompatibleSequenceScenario {
    FITS, ROW_LIMIT_EXACT, ROW_LIMIT_OVERFLOW, BYTE_LIMIT_EXACT, BYTE_LIMIT_OVERFLOW,
    MULTIPLE_BOUNDARIES
  }

  enum SequenceFlushReason {
    FINAL_FLUSH, ROW_LIMIT, ROW_OVERFLOW, BYTE_LIMIT, BYTE_OVERFLOW
  }

  /** Expected frame range and flush event, chosen before any serialization or decoding. */
  static final class ExpectedSequenceBatch {
    final int firstFrame;
    final int endFrame;
    final SequenceFlushReason reason;

    ExpectedSequenceBatch(int firstFrame, int endFrame, SequenceFlushReason reason) {
      this.firstFrame = firstFrame;
      this.endFrame = endFrame;
      this.reason = reason;
    }

    int flushFrame(int frameCount) {
      switch (reason) {
      case FINAL_FLUSH:
        return frameCount;
      case ROW_OVERFLOW:
      case BYTE_OVERFLOW:
        return endFrame;
      case ROW_LIMIT:
      case BYTE_LIMIT:
        return endFrame - 1;
      default:
        throw new AssertionError(reason);
      }
    }
  }

  static final class GeneratedFrame {
    final VectorizedRowBatch source;
    final List<List<NullableValue<?>>> columns;

    GeneratedFrame(VectorizedRowBatch source, List<List<NullableValue<?>>> columns) {
      this.source = source;
      this.columns = columns;
    }
  }

  static final class CompatibleFrameSequence {
    final RandomColumnAdapter<?>[] adapters;
    final List<GeneratedFrame> frames;
    final List<ExpectedSequenceBatch> expectedBatches;
    final int rowCapacity;
    final int paddingColumn;
    final byte paddingByte;
    final String context;

    CompatibleFrameSequence(RandomColumnAdapter<?>[] adapters, List<GeneratedFrame> frames,
        List<ExpectedSequenceBatch> expectedBatches, int rowCapacity, int paddingColumn,
        byte paddingByte, String context) {
      this.adapters = adapters;
      this.frames = frames;
      this.expectedBatches = expectedBatches;
      this.rowCapacity = rowCapacity;
      this.paddingColumn = paddingColumn;
      this.paddingByte = paddingByte;
      this.context = context;
    }

    VectorizedRowBatch newDestination() {
      VectorizedRowBatch destination = new VectorizedRowBatch(adapters.length, rowCapacity);
      for (int column = 0; column < adapters.length; column++) {
        destination.cols[column] = adapters[column].createDestinationVector();
      }
      destination.reset();
      destination.projectionSize = adapters.length;
      for (int column = 0; column < adapters.length; column++) {
        destination.projectedColumns[column] = column;
        setOppositeSequenceMetadata(frames.get(0).source.cols[column], destination.cols[column]);
      }
      return destination;
    }

    /** Expand one non-NULL, non-repeating STRING value without changing frame compatibility. */
    void addPadding(int frameIndex, int extraBytes) {
      assert paddingColumn >= 0;
      assert extraBytes >= 0;
      GeneratedFrame frame = frames.get(frameIndex);
      byte[] original = (byte[]) frame.columns.get(paddingColumn).get(0).value;
      byte[] padded = Arrays.copyOf(original, Math.addExact(original.length, extraBytes));
      Arrays.fill(padded, original.length, padded.length, paddingByte);
      frame.columns.get(paddingColumn).set(0, new NullableValue<>(false, padded));
      int physical = frame.source.selectedInUse ? frame.source.selected[0] : 0;
      ((BytesColumnVector) frame.source.cols[paddingColumn]).setRef(
          physical, padded, 0, padded.length);
    }
  }

  enum CompatibleSequenceSchema {
    PRIMITIVES, DECIMAL_REPRESENTATIONS, DATE_AND_TIMESTAMP, ARRAYS, MAPS,
    STRUCT_AND_UNION, NESTED_COLLECTIONS, METADATA_CHILDREN, NESTED_STRUCT_AND_UNION,
    COMPLEX_MAPS, METADATA_DESCENDANTS
  }

  static void forEachCompatibleFrameSequence(long seed, int iterations,
      CompatibleSequenceScenario scenario, CompatibleFrameSequenceConsumer consumer)
      throws Exception {
    Random random = new Random(seed);
    CompatibleSequenceSchema[] schemas = CompatibleSequenceSchema.values();
    for (int iteration = 0; iteration < iterations; iteration++) {
      CompatibleSequenceSchema schema = schemas[iteration % schemas.length];
      int metadataMode = iteration / schemas.length;
      boolean utc = (metadataMode & 1) != 0;
      boolean proleptic = (metadataMode & 2) != 0;
      int rowCapacity = random.nextBoolean() ? 32 : 64;
      List<Integer> rowCounts = new ArrayList<>();
      List<ExpectedSequenceBatch> expectedBatches = new ArrayList<>();
      int frameCount = 2 + random.nextInt(7);
      switch (scenario) {
      case FITS:
        addSequenceBatch(random, rowCounts, expectedBatches, frameCount,
            fittingSequenceRows(random, frameCount, rowCapacity), SequenceFlushReason.FINAL_FLUSH);
        break;
      case ROW_LIMIT_EXACT:
        addSequenceBatch(random, rowCounts, expectedBatches, frameCount,
            rowCapacity, SequenceFlushReason.ROW_LIMIT);
        break;
      case ROW_LIMIT_OVERFLOW:
        addSequenceBatch(random, rowCounts, expectedBatches, frameCount,
            rowCapacity - 1, SequenceFlushReason.ROW_OVERFLOW);
        addSequenceBatch(random, rowCounts, expectedBatches, 1,
            2 + random.nextInt(7), SequenceFlushReason.FINAL_FLUSH);
        break;
      case BYTE_LIMIT_EXACT:
        addSequenceBatch(random, rowCounts, expectedBatches, frameCount,
            fittingSequenceRows(random, frameCount, rowCapacity), SequenceFlushReason.BYTE_LIMIT);
        break;
      case BYTE_LIMIT_OVERFLOW:
        // Partition all rows together so bytes alone force the first flush.
        addSequenceRows(random, rowCounts, frameCount + 1,
            fittingSequenceRows(random, frameCount + 1, rowCapacity));
        expectedBatches.add(new ExpectedSequenceBatch(0, frameCount,
            SequenceFlushReason.BYTE_OVERFLOW));
        expectedBatches.add(new ExpectedSequenceBatch(frameCount, frameCount + 1,
            SequenceFlushReason.FINAL_FLUSH));
        break;
      case MULTIPLE_BOUNDARIES:
        int groups = 2 + random.nextInt(3);
        for (int group = 0; group < groups; group++) {
          int count = 2 + random.nextInt(3);
          boolean rowBoundary = (iteration + group) % 2 == 0;
          addSequenceBatch(random, rowCounts, expectedBatches, count,
              rowBoundary ? rowCapacity : fittingSequenceRows(random, count, rowCapacity),
              rowBoundary ? SequenceFlushReason.ROW_LIMIT : SequenceFlushReason.BYTE_LIMIT);
        }
        int tailFrames = 2 + random.nextInt(3);
        addSequenceBatch(random, rowCounts, expectedBatches, tailFrames,
            fittingSequenceRows(random, tailFrames, rowCapacity), SequenceFlushReason.FINAL_FLUSH);
        break;
      default:
        throw new AssertionError(scenario);
      }

      RandomColumnAdapter<?>[] adapters = compatibleSequenceAdapters(schema, utc, proleptic);
      boolean byteScenario = scenario == CompatibleSequenceScenario.BYTE_LIMIT_EXACT
          || scenario == CompatibleSequenceScenario.BYTE_LIMIT_OVERFLOW
          || scenario == CompatibleSequenceScenario.MULTIPLE_BOUNDARIES;
      int paddingColumn = byteScenario ? adapters.length : -1;
      if (byteScenario) {
        adapters = Arrays.copyOf(adapters, adapters.length + 1);
        // This extra dense STRING lets the test size real payloads to the serialized-byte limit.
        adapters[paddingColumn] = stringAdapter();
      }
      int repetitionMode = iteration % 4;
      int repeatedNullPattern = iteration / 4 % 3;
      boolean[] repeating = new boolean[adapters.length];
      NullableValue<?>[] representatives = new NullableValue<?>[adapters.length];
      for (int column = 1; column < adapters.length; column++) {
        repeating[column] = column != paddingColumn && (repetitionMode == 1
            || repetitionMode == 2 && column % 2 == 0
            || repetitionMode == 3 && column % 2 != 0);
        if (repeating[column]) {
          boolean isNull = repeatedNullPattern == 1
              || repeatedNullPattern == 2 && column % 2 == 0;
          representatives[column] = new NullableValue<>(isNull, isNull ? null
              : randomAdapterValue(adapters[column], random, null, column + 32));
        }
      }
      List<GeneratedFrame> frames = new ArrayList<>();
      List<String> layouts = new ArrayList<>();
      int nullCase = iteration / 4 % 9;
      int selectionMode = iteration / 36 % 4;
      for (int frame = 0; frame < rowCounts.size(); frame++) {
        // Preserve every pair of NULL patterns and selection modes for the first two frames.
        int nullPattern = frame == 0 ? nullCase / 3
            : frame == 1 ? nullCase % 3 : random.nextInt(3);
        boolean selected = frame < 2 ? (selectionMode & (1 << frame)) != 0 : random.nextBoolean();
        layouts.add("selected=" + selected + "/nullPattern=" + nullPattern);
        frames.add(compatibleSequenceFrame(random, adapters, repeating, representatives,
            rowCounts.get(frame), selected, nullPattern, frame, paddingColumn));
      }
      String context = "compatible sequence seed=" + seed + ", iteration=" + iteration
          + ", scenario=" + scenario + ", schema=" + schema + ", capacity=" + rowCapacity
          + ", rows=" + rowCounts + ", layouts=" + layouts + ", repetitionMode=" + repetitionMode
          + ", repeatingNullPattern=" + repeatedNullPattern
          + ", utc=" + utc + ", proleptic=" + proleptic;
      consumer.accept(new CompatibleFrameSequence(adapters, frames, expectedBatches,
          rowCapacity, paddingColumn, (byte) random.nextInt(256), context));
    }
  }

  static int fittingSequenceRows(Random random, int frameCount, int rowCapacity) {
    int minimum = 2 * frameCount;
    return minimum + random.nextInt(rowCapacity - minimum);
  }

  static void addSequenceBatch(Random random, List<Integer> rowCounts,
      List<ExpectedSequenceBatch> expectedBatches, int frameCount, int rows,
      SequenceFlushReason reason) {
    int firstFrame = rowCounts.size();
    addSequenceRows(random, rowCounts, frameCount, rows);
    expectedBatches.add(new ExpectedSequenceBatch(firstFrame, rowCounts.size(), reason));
  }

  /** Random positive composition with at least two rows in every compact frame. */
  static void addSequenceRows(Random random, List<Integer> rowCounts, int frameCount, int rows) {
    assert rows >= 2 * frameCount;
    int remaining = rows;
    for (int frame = 0; frame < frameCount; frame++) {
      int framesLeft = frameCount - frame - 1;
      int count = framesLeft == 0 ? remaining : 2 + random.nextInt(remaining - 2 * framesLeft - 1);
      rowCounts.add(count);
      remaining -= count;
    }
    assert remaining == 0;
  }

  static GeneratedFrame compatibleSequenceFrame(Random random, RandomColumnAdapter<?>[] adapters,
      boolean[] repeating, NullableValue<?>[] representatives, int rows, boolean selected,
      int nullPattern, int frameNumber, int paddingColumn) {
    RandomBatchLayout layout = new RandomBatchLayout(rows, selected ? shuffledIndices(random) : null);
    VectorizedRowBatch source = new VectorizedRowBatch(adapters.length);
    source.size = rows;
    source.selectedInUse = selected;
    if (selected) {
      System.arraycopy(layout.selected, 0, source.selected, 0, rows);
    }
    List<List<NullableValue<?>>> columns = new ArrayList<>();
    for (int column = 0; column < adapters.length; column++) {
      ColumnVector vector = adapters[column].createVector();
      vector.isRepeating = repeating[column];
      source.cols[column] = vector;
      List<NullableValue<?>> values = new ArrayList<>();
      Object previous = null;
      for (int logical = 0; logical < rows; logical++) {
        NullableValue<?> value;
        if (repeating[column]) {
          value = representatives[column];
        } else if (column == 0) {
          // Disjoint row IDs make lost/reordered/duplicated rows observable across the sequence.
          value = new NullableValue<>(false, frameNumber * 100000L + logical);
        } else {
          // Pattern 2 always contains NULL and non-NULL rows, even for the minimum size two.
          boolean isNull = column != paddingColumn && (nullPattern == 1 || nullPattern == 2
              && (logical < 2 ? logical == (column + frameNumber) % 2 : random.nextInt(5) == 0));
          Object generated = isNull ? null
              : randomAdapterValue(adapters[column], random, previous,
                  logical + 32 + frameNumber * 100);
          value = new NullableValue<>(isNull, generated);
          if (!isNull) {
            previous = generated;
          }
        }
        values.add(value);
        if (!repeating[column] || logical == 0) {
          int physical = repeating[column] ? 0 : layout.sourceIndex(logical);
          if (value.isNull) {
            setNull(vector, physical);
          } else {
            setAdapterValue(adapters[column], vector, physical, value.value);
          }
        }
      }
      columns.add(values);
    }
    return new GeneratedFrame(source, columns);
  }

  static RandomColumnAdapter<?>[] compatibleSequenceAdapters(CompatibleSequenceSchema schema,
      boolean utc, boolean proleptic) {
    // Column zero is always the dense, non-NULL row identifier.
    switch (schema) {
    case PRIMITIVES:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), bigIntAdapter(), doubleAdapter(),
          stringAdapter(), decimalAdapter(), intervalDayTimeAdapter()};
    case DECIMAL_REPRESENTATIONS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), decimalAdapter(false, false),
          decimalAdapter(true, false), decimalAdapter(false, true), decimalAdapter(true, true)};
    case DATE_AND_TIMESTAMP:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), dateAdapter(false), dateAdapter(true),
          timestampAdapter(false, false), timestampAdapter(false, true),
          timestampAdapter(true, false), timestampAdapter(true, true)};
    case ARRAYS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), arrayAdapter(bigIntAdapter()),
          arrayAdapter(stringAdapter()), arrayAdapter(decimalAdapter())};
    case MAPS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(),
          mapAdapter(stringAdapter(), bigIntAdapter()), mapAdapter(bigIntAdapter(), stringAdapter()),
          mapAdapter(stringAdapter(), arrayAdapter(bigIntAdapter()))};
    case STRUCT_AND_UNION:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(),
          structAdapter(bigIntAdapter(), stringAdapter(), decimalAdapter()),
          unionAdapter(bigIntAdapter(), stringAdapter(), decimalAdapter())};
    case NESTED_COLLECTIONS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), arrayAdapter(arrayAdapter(bigIntAdapter())),
          arrayAdapter(mapAdapter(stringAdapter(), bigIntAdapter())),
          mapAdapter(stringAdapter(), mapAdapter(bigIntAdapter(), stringAdapter()))};
    case METADATA_CHILDREN:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(), arrayAdapter(dateAdapter(proleptic)),
          mapAdapter(dateAdapter(!proleptic), timestampAdapter(utc, proleptic)),
          structAdapter(timestampAdapter(!utc, proleptic), dateAdapter(!proleptic)),
          unionAdapter(timestampAdapter(utc, !proleptic), dateAdapter(proleptic))};
    case NESTED_STRUCT_AND_UNION:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(),
          structAdapter(unionAdapter(bigIntAdapter(), stringAdapter()), arrayAdapter(decimalAdapter())),
          unionAdapter(structAdapter(bigIntAdapter(), stringAdapter()), arrayAdapter(decimalAdapter())),
          arrayAdapter(structAdapter(bigIntAdapter(), stringAdapter()))};
    case COMPLEX_MAPS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(),
          mapAdapter(structAdapter(bigIntAdapter(), stringAdapter()),
              unionAdapter(bigIntAdapter(), stringAdapter())),
          arrayAdapter(unionAdapter(bigIntAdapter(), stringAdapter()))};
    case METADATA_DESCENDANTS:
      return new RandomColumnAdapter<?>[] {bigIntAdapter(),
          structAdapter(arrayAdapter(dateAdapter(proleptic)),
              unionAdapter(timestampAdapter(utc, !proleptic), dateAdapter(!proleptic))),
          arrayAdapter(unionAdapter(dateAdapter(!proleptic), timestampAdapter(!utc, proleptic))),
          mapAdapter(dateAdapter(proleptic), arrayAdapter(timestampAdapter(utc, proleptic)))};
    default:
      throw new AssertionError(schema);
    }
  }

  static void setOppositeSequenceMetadata(ColumnVector source, ColumnVector destination) {
    if (source instanceof DateColumnVector) {
      ((DateColumnVector) destination).setUsingProlepticCalendar(
          !((DateColumnVector) source).isUsingProlepticCalendar());
    } else if (source instanceof TimestampColumnVector) {
      TimestampColumnVector sourceTimestamp = (TimestampColumnVector) source;
      TimestampColumnVector destinationTimestamp = (TimestampColumnVector) destination;
      destinationTimestamp.setIsUTC(!sourceTimestamp.isUTC());
      destinationTimestamp.setUsingProlepticCalendar(!sourceTimestamp.usingProlepticCalendar());
    } else if (source instanceof ListColumnVector) {
      setOppositeSequenceMetadata(((ListColumnVector) source).child,
          ((ListColumnVector) destination).child);
    } else if (source instanceof MapColumnVector) {
      setOppositeSequenceMetadata(((MapColumnVector) source).keys,
          ((MapColumnVector) destination).keys);
      setOppositeSequenceMetadata(((MapColumnVector) source).values,
          ((MapColumnVector) destination).values);
    } else if (source instanceof StructColumnVector || source instanceof UnionColumnVector) {
      ColumnVector[] sourceFields = source instanceof StructColumnVector
          ? ((StructColumnVector) source).fields : ((UnionColumnVector) source).fields;
      ColumnVector[] destinationFields = destination instanceof StructColumnVector
          ? ((StructColumnVector) destination).fields : ((UnionColumnVector) destination).fields;
      for (int field = 0; field < sourceFields.length; field++) {
        setOppositeSequenceMetadata(sourceFields[field], destinationFields[field]);
      }
    }
  }

  static void assertCompatibleFrameSequenceResult(CompatibleFrameSequence sequence,
      VectorizedRowBatch actual, int firstFrame, int endFrame, String context) {
    assertTrue(context + ", frame range", firstFrame >= 0 && firstFrame < endFrame
        && endFrame <= sequence.frames.size());
    int rows = 0;
    for (int frame = firstFrame; frame < endFrame; frame++) {
      rows += sequence.frames.get(frame).columns.get(0).size();
    }
    assertEquals(context + ", rows", rows, actual.size);
    assertEquals(context + ", projection size", sequence.adapters.length, actual.projectionSize);
    assertFalse(context + ", compact destination", actual.selectedInUse);
    List<Integer> positions = new ArrayList<>(rows);
    for (int row = 0; row < rows; row++) {
      positions.add(row);
    }
    for (int column = 0; column < sequence.adapters.length; column++) {
      String columnContext = context + ", column=" + column;
      assertEquals(columnContext + ", projection", column, actual.projectedColumns[column]);
      List<NullableValue<?>> expected = new ArrayList<>(rows);
      for (int frame = firstFrame; frame < endFrame; frame++) {
        expected.addAll(sequence.frames.get(frame).columns.get(column));
      }
      assertEquals(columnContext + ", expected rows", rows, expected.size());
      ColumnVector sourceColumn = sequence.frames.get(firstFrame).source.cols[column];
      ColumnVector actualColumn = actual.cols[column];
      boolean repeating = sourceColumn.isRepeating;
      assertEquals(columnContext + ", root repetition", repeating, actualColumn.isRepeating);
      for (int row = 0; row < rows; row++) {
        NullableValue<?> value = expected.get(row);
        int index = repeating ? 0 : row;
        String rowContext = columnContext + ", row=" + row;
        assertEquals(rowContext + ", NULL", value.isNull, isNull(actualColumn, index));
        if (!value.isNull) {
          assertAdapterValue(sequence.adapters[column], rowContext, value.value, actualColumn, index);
        }
      }
      // Recheck the entire accumulated range, including all collection descendants.
      assertNestedRepeatingLayout(columnContext, sourceColumn, actualColumn, expected,
          positions, repeating);
      assertCompatibleColumnInvariants(sequence.adapters[column], columnContext, expected,
          repeating, actualColumn);
      // Metadata is carried even by empty collections and inactive union alternatives.
      for (int frame = firstFrame; frame < endFrame; frame++) {
        assertCompatibleFrameMetadata(columnContext + ", frame=" + frame,
            sequence.frames.get(frame).source.cols[column], actualColumn);
      }
    }
  }

  @SuppressWarnings("unchecked")
  static void assertCompatibleColumnInvariants(RandomColumnAdapter<?> adapter, String context,
      List<NullableValue<?>> expected, boolean repeating, ColumnVector actual) {
    List<Object> values = new ArrayList<>(expected.size());
    boolean[] nulls = new boolean[repeating ? Math.min(expected.size(), 1) : expected.size()];
    for (int row = 0; row < expected.size(); row++) {
      values.add(expected.get(row).value);
      if (row < nulls.length) {
        nulls[row] = expected.get(row).isNull;
      }
    }
    RandomColumnScenario scenario = new RandomColumnScenario(
        new RandomBatchLayout(expected.size(), null), new RandomColumnLayout(repeating, nulls));
    ((RandomColumnAdapter<Object>) adapter).assertColumnInvariants(context, values, scenario, actual);
  }

  static void assertCompatibleFrameMetadata(String context, ColumnVector expected,
      ColumnVector actual) {
    if (expected instanceof DateColumnVector) {
      assertTrue(context + ", DATE vector", actual instanceof DateColumnVector);
      assertEquals(context + ", calendar", ((DateColumnVector) expected).isUsingProlepticCalendar(),
          ((DateColumnVector) actual).isUsingProlepticCalendar());
    } else if (expected instanceof TimestampColumnVector) {
      assertTrue(context + ", TIMESTAMP vector", actual instanceof TimestampColumnVector);
      assertEquals(context + ", UTC", ((TimestampColumnVector) expected).isUTC(),
          ((TimestampColumnVector) actual).isUTC());
      assertEquals(context + ", calendar", ((TimestampColumnVector) expected).usingProlepticCalendar(),
          ((TimestampColumnVector) actual).usingProlepticCalendar());
    } else if (expected instanceof DecimalColumnVector || expected instanceof Decimal64ColumnVector) {
      assertTrue(context + ", DECIMAL vector", actual instanceof DecimalColumnVector
          || actual instanceof Decimal64ColumnVector);
      int expectedPrecision = expected instanceof DecimalColumnVector
          ? ((DecimalColumnVector) expected).precision : ((Decimal64ColumnVector) expected).precision;
      int expectedScale = expected instanceof DecimalColumnVector
          ? ((DecimalColumnVector) expected).scale : ((Decimal64ColumnVector) expected).scale;
      int actualPrecision = actual instanceof DecimalColumnVector
          ? ((DecimalColumnVector) actual).precision : ((Decimal64ColumnVector) actual).precision;
      int actualScale = actual instanceof DecimalColumnVector
          ? ((DecimalColumnVector) actual).scale : ((Decimal64ColumnVector) actual).scale;
      assertEquals(context + ", precision", expectedPrecision, actualPrecision);
      assertEquals(context + ", scale", expectedScale, actualScale);
    } else if (expected instanceof ListColumnVector) {
      assertTrue(context + ", ARRAY vector", actual instanceof ListColumnVector);
      assertCompatibleFrameMetadata(context + "/element", ((ListColumnVector) expected).child,
          ((ListColumnVector) actual).child);
    } else if (expected instanceof MapColumnVector) {
      assertTrue(context + ", MAP vector", actual instanceof MapColumnVector);
      assertCompatibleFrameMetadata(context + "/key", ((MapColumnVector) expected).keys,
          ((MapColumnVector) actual).keys);
      assertCompatibleFrameMetadata(context + "/value", ((MapColumnVector) expected).values,
          ((MapColumnVector) actual).values);
    } else if (expected instanceof StructColumnVector || expected instanceof UnionColumnVector) {
      boolean struct = expected instanceof StructColumnVector;
      assertTrue(context + ", complex vector", struct ? actual instanceof StructColumnVector
          : actual instanceof UnionColumnVector);
      ColumnVector[] expectedFields = struct ? ((StructColumnVector) expected).fields
          : ((UnionColumnVector) expected).fields;
      ColumnVector[] actualFields = struct ? ((StructColumnVector) actual).fields
          : ((UnionColumnVector) actual).fields;
      assertEquals(context + ", field count", expectedFields.length, actualFields.length);
      for (int field = 0; field < expectedFields.length; field++) {
        assertCompatibleFrameMetadata(context + "/field=" + field,
            expectedFields[field], actualFields[field]);
      }
    }
  }

  @FunctionalInterface
  interface ColumnCaseConsumer<T> {
    void accept(List<T> expected, RandomColumnScenario scenario, VectorizedRowBatch source,
        String context) throws Exception;
  }

  @FunctionalInterface
  interface BytesSliceCaseConsumer {
    void accept(RandomBatchLayout layout, VectorizedRowBatch source, List<byte[]> expected,
        String context) throws Exception;
  }

  @FunctionalInterface
  interface ColumnMappingCaseConsumer {
    void accept(RandomBatchLayout layout, int[] mapping, VectorizedRowBatch source,
        String context) throws Exception;
  }

  @FunctionalInterface
  interface NestedRepeatingCaseConsumer {
    void accept(NestedRepeatingCase[] scenarios, VectorizedRowBatch source,
        boolean expectedNestedRepeating, String context) throws Exception;
  }

  static <T> void forEachColumnCase(String typeName, long seed, int iterations,
      RandomColumnAdapter<T> adapter, String logPrefix, ColumnCaseConsumer<T> consumer)
      throws Exception {
    Random random = new Random(seed);
    try (RandomColumnLog<T> output = newColumnLog(logPrefix, typeName, seed, adapter)) {
      for (int iteration = 0; iteration < iterations; iteration++) {
        RandomColumnScenario scenario = adapter.randomScenario(random, iteration);
        List<T> expected = randomValues(random, scenario, adapter, iteration);
        output.write(iteration, scenario, expected);
        ColumnVector sourceColumn = adapter.createVector();
        scenario.columnLayout.apply(sourceColumn, scenario.batchLayout);
        for (int logical = 0; logical < scenario.valueCount(); logical++) {
          if (!scenario.isNull(logical)) {
            adapter.setValue(sourceColumn, scenario.sourceIndex(logical), expected.get(logical));
          }
        }

        String context = typeName + ", seed=" + seed + ", iteration=" + iteration + ", " + scenario;
        consumer.accept(expected, scenario, scenario.batchLayout.batchWithColumn(sourceColumn),
            context);
      }
    }
  }

  static void forEachBytesSliceCase(long seed, int iterations,
      BytesSliceCaseConsumer consumer) throws Exception {
    Random random = new Random(seed);
    for (int iteration = 0; iteration < iterations; iteration++) {
      RandomBatchLayout layout = randomBatchLayout(random, iteration);
      List<byte[]> expected = randomSliceValues(random, layout.size, iteration);
      BytesColumnVector source = new BytesColumnVector();
      applySliceValues(source, layout, expected, random, iteration);

      consumer.accept(layout, layout.batchWithColumn(source), expected,
          "bytes slice seed=" + seed + ", iteration=" + iteration + ", " + layout);
    }
  }

  static void forEachColumnMappingCase(long seed, int iterations,
      ColumnMappingCaseConsumer consumer) throws Exception {
    Random random = new Random(seed);
    for (int iteration = 0; iteration < iterations; iteration++) {
      RandomBatchLayout layout = randomBatchLayout(random, iteration);
      // Guarantee nonempty batches with empty projections, with and without selection.
      if (iteration == 6 || iteration == 7) {
        layout = new RandomBatchLayout(17, iteration == 7 ? shuffledIndices(random) : null);
      }
      int sourceColumnCount = randomMappingSourceColumnCount(random, iteration);
      int[] sourceColumnMap = randomSourceColumnMap(random, iteration, sourceColumnCount);
      VectorizedRowBatch source = randomMappingSourceBatch(layout, sourceColumnCount);
      consumer.accept(layout, sourceColumnMap, source,
          "column mapping seed=" + seed + ", iteration=" + iteration + ", " + layout
              + ", sourceColumnMap=" + Arrays.toString(sourceColumnMap));
    }
  }

  static void forEachNestedRepeatingCase(NestedRepeatingCaseConsumer consumer)
      throws Exception {
    Random random = new Random(NESTED_REPEATING_PROPERTY_TEST_SEED);
    for (int iteration = 0; iteration < NESTED_COMPLEX_PROPERTY_TEST_ITERATIONS; iteration++) {
      int depth = iteration % 4;
      int enclosingDepth = iteration / 16 % 4;
      NestedRepeatingValue payload = nestedRepeatingPayload(random, iteration, depth);
      for (NestedRepeatingContainer container : NestedRepeatingContainer.values()) {
        for (int children : new int[] {0, 1, 2, 3}) {
          for (int rows : new int[] {1, 3}) {
            for (boolean repeating : new boolean[] {false, true}) {
              // Struct fields and union alternatives get one invocation per active parent.
              if ((container == NestedRepeatingContainer.STRUCT
                  || container == NestedRepeatingContainer.UNION)
                  && children > (repeating ? 1 : rows)) {
                continue;
              }
              for (boolean selected : new boolean[] {false, true}) {
                for (boolean childNull : new boolean[] {false, true}) {
                  NestedRepeatingCase scenario = nestedRepeatingCase(random, container,
                      payload, children, rows, repeating, selected, childNull);
                  if (!repeating) {
                    // Push the decisive repetition below ordinary ancestors, so checking
                    // only immediate children cannot satisfy the header-flag property.
                    scenario = encloseNestedRepeatingCase(scenario, enclosingDepth);
                  }
                  String context = "nested repeating seed=" + NESTED_REPEATING_PROPERTY_TEST_SEED
                      + ", iteration=" + iteration + ", depth=" + depth + ", root=" + container
                      + ", enclosingDepth=" + (repeating ? 0 : enclosingDepth)
                      + ", children=" + children + ", rows=" + rows + ", repeating=" + repeating
                      + ", selected=" + selected + ", childNull=" + childNull;
                  consumer.accept(new NestedRepeatingCase[] {scenario}, scenario.source,
                      children > 1, context);
                }
              }
            }
          }
        }
      }
      // Cover simultaneous singleton normalization and struct/union row mappings, too.
      NestedRepeatingCase[] singletons = new NestedRepeatingCase[4];
      ColumnVector[] columns = new ColumnVector[4];
      for (NestedRepeatingContainer container : NestedRepeatingContainer.values()) {
        int column = container.ordinal();
        singletons[column] = nestedRepeatingCase(random, container, payload,
            1, 2, false, false, false);
        columns[column] = singletons[column].source.cols[0];
      }
      consumer.accept(singletons, batch(2, columns), false,
          "nested repeating seed=" + NESTED_REPEATING_PROPERTY_TEST_SEED
              + ", iteration=" + iteration + ", depth=" + depth + ", mixed singletons");
    }
  }
}
