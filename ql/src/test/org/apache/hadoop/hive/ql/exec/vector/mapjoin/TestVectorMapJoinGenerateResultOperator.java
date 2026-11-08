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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorCopyRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorVerifyFast;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestVectorMapJoinGenerateResultOperator {
  private static final TypeInfo[] PRIMITIVE_TYPES = types(
      "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "string",
      "binary", "char(5)", "varchar(5)", "decimal(18,3)", "date", "timestamp",
      "interval_year_month", "interval_day_time");

  private static class TestOperator extends VectorMapJoinGenerateResultOperator {
    int deserializeCount;

    void configure(TypeInfo[] types) throws HiveException {
      smallTableValueMapping = new VectorColumnSourceMapping("test values");
      for (int i = 0; i < types.length; i++) {
        smallTableValueMapping.add(i, valueColumn(i), types[i]);
      }
      smallTableValueMapping.finalize();
      initializeSmallTableValueDeserializeRow();
    }

    @Override
    protected void doSmallTableValueDeserializeRow(VectorizedRowBatch batch, int row,
        ByteSegmentRef bytes, VectorMapJoinHashMapResult result) throws HiveException {
      deserializeCount++;
      super.doSmallTableValueDeserializeRow(batch, row, bytes, result);
    }

    @Override
    protected String getLoggingPrefix() {
      return "test";
    }

    @Override
    public void processBatch(VectorizedRowBatch batch) {
      throw new UnsupportedOperationException();
    }
  }

  private static TypeInfo[] types(String... names) {
    TypeInfo[] types = new TypeInfo[names.length];
    for (int i = 0; i < names.length; i++) {
      types[i] = TypeInfoUtils.getTypeInfoFromTypeString(names[i]);
    }
    return types;
  }

  private static int valueColumn(int field) {
    return 2 + 2 * field;
  }

  private static VectorizedRowBatch batch(TypeInfo[] types) throws HiveException {
    VectorizedRowBatch batch = new VectorizedRowBatch(2 + 2 * types.length);
    // Interleave value outputs with unrelated big-table columns to catch mapping mistakes.
    for (int c = 0; c < batch.numCols; c++) {
      LongColumnVector column = new LongColumnVector();
      for (int r = 0; r < column.vector.length; r++) {
        column.vector[r] = 10000L * c + r;
      }
      batch.cols[c] = column;
    }
    for (int i = 0; i < types.length; i++) {
      ColumnVector column = VectorizedBatchUtil.createColumnVector(types[i]);
      column.init();
      Arrays.fill(column.isNull, true);
      column.noNulls = false;
      batch.cols[valueColumn(i)] = column;
    }
    return batch;
  }

  private static VectorMapJoinHashMapResult result(TypeInfo[] types, Object[] row)
      throws Exception {
    LazyBinarySerializeWrite writer = new LazyBinarySerializeWrite(types.length);
    Output output = new Output();
    writer.set(output);
    for (int i = 0; i < types.length; i++) {
      VectorVerifyFast.serializeWrite(writer, types[i], row[i]);
    }
    // Include a nonzero segment offset, as in a hash-table WriteBuffers segment.
    byte[] bytes = new byte[output.getLength() + 7];
    System.arraycopy(output.getData(), 0, bytes, 3, output.getLength());
    ByteSegmentRef ref = mock(ByteSegmentRef.class);
    when(ref.getBytes()).thenReturn(bytes);
    when(ref.getOffset()).thenReturn(3L);
    when(ref.getLength()).thenReturn(output.getLength());
    VectorMapJoinHashMapResult result = mock(VectorMapJoinHashMapResult.class);
    when(result.first()).thenReturn(ref);
    return result;
  }

  private static Object[] randomRow(TypeInfo[] types) {
    Random random = new Random(79411);
    Object[] row = new Object[types.length];
    for (int i = 0; i < types.length; i++) {
      row[i] = VectorRandomRowSource.randomWritable(random, types[i],
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(types[i]));
    }
    return row;
  }

  private static Object[] extract(TypeInfo[] types, VectorizedRowBatch batch, int row)
      throws HiveException {
    int[] columns = new int[types.length];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = valueColumn(i);
    }
    VectorExtractRow extract = new VectorExtractRow();
    extract.init(types, columns);
    Object[] values = new Object[types.length];
    extract.extractRow(batch, row, values);
    return values;
  }

  @Test
  public void testPrimitiveValuesNullsAndColumnIsolation() throws Exception {
    TestOperator operator = new TestOperator();
    operator.configure(PRIMITIVE_TYPES);
    assertNotNull(operator.smallTableValueVectorCopy);
    operator.outerSmallTableKeyVectorCopy = new VectorCopyRow();
    operator.outerSmallTableKeyVectorCopy.init(new int[] {0}, new int[] {1}, types("bigint"));
    TestOperator reference = new TestOperator();
    reference.configure(PRIMITIVE_TYPES);
    VectorizedRowBatch actual = batch(PRIMITIVE_TYPES);
    VectorizedRowBatch expected = batch(PRIMITIVE_TYPES);
    for (int i = 0; i < PRIMITIVE_TYPES.length; i++) {
      actual.cols[valueColumn(i)].noNulls = true;
    }
    actual.selected[0] = 21;
    actual.selected[1] = 22;
    int[] matches = {99, 17, 4, 11, 98};

    // Reuse destinations across nonnull -> mixed null -> all null -> nonnull transitions.
    for (int mode : new int[] {0, 1, 2, 0}) {
      Object[] row = randomRow(PRIMITIVE_TYPES);
      for (int i = 0; i < row.length; i++) {
        if (mode == 2 || (mode == 1 && i % 2 == 0)) {
          row[i] = null;
        }
      }
      VectorMapJoinHashMapResult result = result(PRIMITIVE_TYPES, row);
      int before = operator.deserializeCount;
      assertEquals(5, operator.generateHashMapResultSingleValue(
          actual, result, matches, 1, 3, 2));
      assertEquals(before + 1, operator.deserializeCount);
      assertArrayEquals(new int[] {21, 22, 17, 4, 11}, Arrays.copyOf(actual.selected, 5));
      reference.doSmallTableValueDeserializeRow(expected, 17, result.first(), result);
      Object[] expectedValues = extract(PRIMITIVE_TYPES, expected, 17);
      for (int r : new int[] {17, 4, 11}) {
        assertArrayEquals(expectedValues, extract(PRIMITIVE_TYPES, actual, r));
        assertEquals(r, ((LongColumnVector) actual.cols[1]).vector[r]);
        for (int i = 0; i < row.length; i++) {
          ColumnVector column = actual.cols[valueColumn(i)];
          assertEquals(row[i] == null, column.isNull[r]);
          assertFalse(column.isRepeating);
          if (row[i] == null) {
            assertFalse(column.noNulls);
          } else if (column instanceof BytesColumnVector) {
            BytesColumnVector bytes = (BytesColumnVector) column;
            assertSame(result.first().getBytes(), bytes.vector[r]);
            assertEquals(bytes.start[17], bytes.start[r]);
            assertEquals(bytes.length[17], bytes.length[r]);
          }
        }
      }
    }
    for (int r = 0; r < VectorizedRowBatch.DEFAULT_SIZE; r++) {
      assertEquals(r, ((LongColumnVector) actual.cols[0]).vector[r]);
      for (int i = 0; i < PRIMITIVE_TYPES.length; i++) {
        int c = valueColumn(i) + 1;
        assertEquals(10000L * c + r, ((LongColumnVector) actual.cols[c]).vector[r]);
      }
    }
    DecimalColumnVector decimals = (DecimalColumnVector) actual.cols[valueColumn(11)];
    assertNotSame(decimals.vector[17], decimals.vector[4]);
    String retained = decimals.vector[4].toString();
    decimals.vector[17].setFromLong(0);
    assertEquals(retained, decimals.vector[4].toString());
  }

  @Test
  public void testDecimal64Values() throws Exception {
    TypeInfo[] types = types("decimal(18,3)");
    TestOperator operator = new TestOperator();
    operator.configure(types);
    VectorizedRowBatch batch = batch(types);
    batch.cols[2] = VectorizedBatchUtil.createColumnVector(
        types[0], DataTypePhysicalVariation.DECIMAL_64);
    Object[] values = randomRow(types);
    operator.generateHashMapResultSingleValue(
        batch, result(types, values), new int[] {7, 2}, 0, 2, 0);
    assertEquals(1, operator.deserializeCount);
    assertArrayEquals(values, extract(types, batch, 7));
    assertArrayEquals(values, extract(types, batch, 2));
  }

  @Test
  public void testByteReferencesSurviveLaterDeserialization() throws Exception {
    TypeInfo[] types = types("string");
    TestOperator operator = new TestOperator();
    operator.configure(types);
    VectorizedRowBatch batch = batch(types);
    VectorMapJoinHashMapResult first = result(types, new Object[] {new Text("first value")});
    operator.generateHashMapResultSingleValue(batch, first, new int[] {9, 2}, 0, 2, 0);
    byte[] retained = ((BytesColumnVector) batch.cols[2]).vector[2];
    operator.generateHashMapResultSingleValue(batch,
        result(types, new Object[] {new Text("different value")}), new int[] {3, 6}, 0, 2, 2);
    assertEquals(2, operator.deserializeCount);
    assertSame(retained, ((BytesColumnVector) batch.cols[2]).vector[2]);
    assertArrayEquals(new Object[] {new Text("first value")}, extract(types, batch, 2));
    assertArrayEquals(new Object[] {new Text("different value")}, extract(types, batch, 6));
  }

  @Test
  public void testZeroSingleAndNoValueColumns() throws Exception {
    TypeInfo[] types = types("bigint");
    TestOperator operator = new TestOperator();
    operator.configure(types);
    VectorizedRowBatch batch = batch(types);
    VectorMapJoinHashMapResult result = result(types, randomRow(types));
    assertEquals(3, operator.generateHashMapResultSingleValue(batch, result, new int[0], 0, 0, 3));
    assertEquals(0, operator.deserializeCount);
    assertEquals(1, operator.generateHashMapResultSingleValue(batch, result, new int[] {7}, 0, 1, 0));
    assertEquals(1, operator.deserializeCount);
    operator.configure(new TypeInfo[0]);
    assertNull(operator.smallTableValueVectorDeserializeRow);
    assertNull(operator.smallTableValueVectorCopy);
    assertEquals(3, operator.generateHashMapResultSingleValue(batch, result, new int[] {8, 5}, 0, 2, 1));
    assertEquals(1, operator.deserializeCount);
    assertArrayEquals(new int[] {7, 8, 5}, Arrays.copyOf(batch.selected, 3));
  }

  @Test
  public void testComplexValuesKeepPerRowDeserialization() throws Exception {
    TypeInfo[] types = types("bigint", "array<string>");
    TestOperator operator = new TestOperator();
    operator.configure(types);
    assertNull(operator.smallTableValueVectorCopy);
    VectorizedRowBatch actual = batch(types);
    Object[] values = randomRow(types);
    values[1] = new ArrayList<>(Arrays.asList(new Text("a"), null, new Text("b")));
    VectorMapJoinHashMapResult result = result(types, values);
    operator.generateHashMapResultSingleValue(actual, result, new int[] {8, 3, 6}, 0, 3, 0);
    assertEquals(3, operator.deserializeCount);
    for (int row : new int[] {8, 3, 6}) {
      assertArrayEquals(values, extract(types, actual, row));
    }
  }

  @Test
  public void testVoidSchemaDisablesCopyAndReinitializationClearsHelpers() throws Exception {
    TestOperator operator = new TestOperator();
    operator.configure(types("bigint"));
    assertNotNull(operator.smallTableValueVectorCopy);
    operator.configure(types("void"));
    assertNotNull(operator.smallTableValueVectorDeserializeRow);
    assertNull(operator.smallTableValueVectorCopy);
    operator.configure(new TypeInfo[0]);
    assertNull(operator.smallTableValueVectorDeserializeRow);
    assertNull(operator.smallTableValueVectorCopy);
  }
}
