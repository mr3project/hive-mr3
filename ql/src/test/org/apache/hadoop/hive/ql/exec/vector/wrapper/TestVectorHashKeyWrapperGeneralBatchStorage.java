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
package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.common.util.Murmur3;
import org.junit.Test;

public class TestVectorHashKeyWrapperGeneralBatchStorage {

  // Deliberately interleave types so logical null indices differ from value indices.
  private static final TypeInfo[] TYPES = {
      TypeInfoFactory.stringTypeInfo, TypeInfoFactory.getDecimalTypeInfo(20, 4),
      TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.timestampTypeInfo,
      TypeInfoFactory.longTypeInfo, TypeInfoFactory.intervalDayTimeTypeInfo,
      TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
      TypeInfoFactory.doubleTypeInfo};

  @Test
  public void testAllTypesWithSelectionRepeatingAndNulls() throws HiveException {
    for (boolean selected : new boolean[] {false, true}) {
      for (boolean repeating : new boolean[] {false, true}) {
        for (boolean nullable : new boolean[] {false, true}) {
          VectorHashKeyWrapperBatch wrappers = compile();
          VectorizedRowBatch batch = batch(selected, repeating, nullable);
          wrappers.evaluateBatch(batch);
          assertRows(wrappers, batch, null);

          // Reuse the same row views after nulls and repeating values disappear.
          batch = batch(!selected, false, false);
          wrappers.evaluateBatch(batch);
          assertRows(wrappers, batch, null);
        }
      }
    }
  }

  @Test
  public void testGroupingSetOverridesEveryTypeAndClearsNulls() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = compile();
    VectorizedRowBatch batch = batch(true, false, false);
    for (int key = 0; key < TYPES.length; ++key) {
      boolean[] overrides = new boolean[TYPES.length];
      overrides[key] = true;
      wrappers.evaluateBatchGroupingSets(batch, overrides);
      assertRows(wrappers, batch, overrides);
    }
    boolean[] allNull = new boolean[TYPES.length];
    Arrays.fill(allNull, true);
    wrappers.evaluateBatchGroupingSets(batch, allNull);
    assertRows(wrappers, batch, allNull);
    wrappers.evaluateBatch(batch);
    assertRows(wrappers, batch, null);
  }

  @Test
  public void testRetainedKeysOwnAllMutableValuesAndCanBeReused() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = compile();
    VectorizedRowBatch batch = batch(false, false, false);
    wrappers.evaluateBatch(batch);
    VectorHashKeyWrapperBase view = wrappers.getVectorHashKeyWrappers()[0];
    VectorHashKeyWrapperGeneral expected =
        (VectorHashKeyWrapperGeneral) expected(batch, 0, null).copyKey();
    VectorHashKeyWrapperGeneral retained = (VectorHashKeyWrapperGeneral) view.copyKey();
    assertNotSame(view.getBytes(0), retained.getBytes(0));
    assertNotSame(view.getDecimal(0), retained.getDecimal(0));
    assertNotSame(view.getTimestamp(0), retained.getTimestamp(0));
    assertNotSame(view.getIntervalDayTime(0), retained.getIntervalDayTime(0));

    Map<VectorHashKeyWrapperBase, String> map = new HashMap<>();
    map.put(retained, "retained");
    assertEquals("retained", map.get(view));

    ((BytesColumnVector) batch.cols[0]).vector[0][2] ^= 1;
    ((DecimalColumnVector) batch.cols[1]).vector[0].set(HiveDecimal.create("999"));
    ((TimestampColumnVector) batch.cols[3]).set(0, new Timestamp(999));
    ((IntervalDayTimeColumnVector) batch.cols[5]).set(0, new HiveIntervalDayTime(999, 7));
    assertEquals(expected.getDecimal(0), view.getDecimal(0));
    assertEquals(expected.getTimestamp(0), view.getTimestamp(0));
    assertEquals(expected.getIntervalDayTime(0), view.getIntervalDayTime(0));
    wrappers.evaluateBatch(batch);
    assertEquals(expected, retained);
    assertFalse(retained.equals(view));

    // Updating a retained key must copy its objects as well as its string slices.
    map.clear();
    view.copyKey(retained);
    assertEquivalent(view, retained);
    VectorHashKeyWrapperBase snapshot = (VectorHashKeyWrapperBase) retained.copyKey();
    view.getDecimal(0).set(HiveDecimal.ZERO);
    view.getTimestamp(0).setTime(0);
    view.getIntervalDayTime(0).set(0, 0);
    assertEquals(snapshot, retained);
  }

  @Test
  public void testRepeatingMutableSlotsAreIndependent() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = compile();
    VectorizedRowBatch batch = batch(false, true, false);
    wrappers.evaluateBatch(batch);
    VectorHashKeyWrapperBase first = wrappers.getVectorHashKeyWrappers()[0];
    VectorHashKeyWrapperBase second = wrappers.getVectorHashKeyWrappers()[1];
    VectorHashKeyWrapperGeneral expected = expected(batch, 0, null);
    assertNotSame(first.getDecimal(0), second.getDecimal(0));
    assertNotSame(first.getTimestamp(0), second.getTimestamp(0));
    assertNotSame(first.getIntervalDayTime(0), second.getIntervalDayTime(0));
    first.getDecimal(0).set(HiveDecimal.ZERO);
    first.getTimestamp(0).setTime(0);
    first.getIntervalDayTime(0).set(0, 0);
    assertEquivalent(expected, second);
  }

  @Test
  public void testDoubleNaNsAndSignedZero() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        new VectorExpression[] {new IdentityExpression(0)},
        new TypeInfo[] {TypeInfoFactory.doubleTypeInfo});
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    DoubleColumnVector doubles = new DoubleColumnVector();
    batch.cols[0] = doubles;
    batch.size = 4;
    doubles.vector[0] = Double.NaN;
    doubles.vector[1] = Double.longBitsToDouble(0x7ff0000000000001L);
    doubles.vector[2] = 0.0;
    doubles.vector[3] = -0.0;
    wrappers.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] rows = wrappers.getVectorHashKeyWrappers();
    assertEquivalent(rows[0], rows[1]);
    assertEquivalent(rows[0], (VectorHashKeyWrapperBase) rows[1].copyKey());
    assertFalse(rows[2].equals(rows[3]));
    assertFalse(rows[2].equals(rows[3].copyKey()));
    assertFalse(rows[3].copyKey().equals(rows[2]));
  }

  @Test
  public void testStringHashFastPathsMatchIncrementalHash() throws HiveException {
    TypeInfo[] types = {TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo};
    VectorExpression[] expressions = {new IdentityExpression(0), new IdentityExpression(1),
        new IdentityExpression(2), new IdentityExpression(3)};
    VectorHashKeyWrapperBatch wrappers =
        VectorHashKeyWrapperBatch.compileKeyWrapperBatch(expressions, types);
    String[] choices = {null, "", "a", "a string that spans more than sixteen bytes"};
    for (String s0 : choices) {
      for (String s1 : choices) {
        for (String s2 : choices) {
          VectorHashKeyWrapperGeneral owned = new VectorHashKeyWrapperGeneral(
              new VectorHashKeyWrapperBase.HashContext(), 0, 1, 3, 0, 0, 0, 4);
          VectorizedRowBatch batch = new VectorizedRowBatch(4);
          batch.size = 1;
          batch.cols[0] = new DoubleColumnVector();
          owned.assignDouble(0, -0.0);
          ((DoubleColumnVector) batch.cols[0]).vector[0] = -0.0;
          String[] strings = {s0, s1, s2};
          boolean[] nulls = new boolean[4];
          int seed = Arrays.hashCode(new long[0]) ^ Arrays.hashCode(new double[] {-0.0});
          for (int i = 0; i < strings.length; ++i) {
            BytesColumnVector column = new BytesColumnVector();
            batch.cols[i + 1] = column;
            if (strings[i] == null) {
              column.noNulls = false;
              column.isNull[0] = true;
              nulls[i + 1] = true;
              owned.assignNullString(i + 1, i);
            } else {
              byte[] bytes = ("__" + strings[i] + "__").getBytes(StandardCharsets.UTF_8);
              column.setRef(0, bytes, 2, bytes.length - 4);
              owned.assignString(i, bytes, 2, bytes.length - 4);
            }
          }
          seed ^= Arrays.hashCode(nulls);
          Murmur3.IncrementalHash32 incremental = new Murmur3.IncrementalHash32();
          incremental.start(seed);
          boolean hasNonNull = false;
          for (int i = 0; i < strings.length; ++i) {
            if (strings[i] != null) {
              incremental.add(owned.getBytes(i), owned.getByteStart(i), owned.getByteLength(i));
              hasNonNull = true;
            }
          }
          int expectedHash = hasNonNull ? incremental.end() : seed;
          owned.setHashKey();
          assertEquals(expectedHash, owned.hashCode());
          wrappers.evaluateBatch(batch);
          VectorHashKeyWrapperBase view = wrappers.getVectorHashKeyWrappers()[0];
          assertEquals(expectedHash, view.hashCode());
          assertEquivalent(owned, view);
        }
      }
    }
  }

  @Test
  public void testStringCopyReusesSliceCapacityAndReleasesNullBuffers() {
    VectorHashKeyWrapperGeneral source = new VectorHashKeyWrapperGeneral(
        new VectorHashKeyWrapperBase.HashContext(), 0, 1, 1, 0, 0, 0, 2);
    source.assignString(0, new byte[32], 0, 32);
    source.setHashKey();
    VectorHashKeyWrapperGeneral retained = (VectorHashKeyWrapperGeneral) source.copyKey();
    byte[] retainedBuffer = retained.getBytes(0);
    source.assignString(0, new byte[65536], 100, 20);
    source.setHashKey();
    source.copyKey(retained);
    assertSame(retainedBuffer, retained.getBytes(0));
    assertEquivalent(source, retained);
    source.assignNullString(1, 0);
    source.setHashKey();
    source.copyKey(retained);
    assertNull(retained.getBytes(0));
    assertEquivalent(source, retained);
  }

  @Test
  public void testRowEqualityAcrossCapacitiesAndLayouts() {
    VectorHashKeyWrapperBase.HashContext context = new VectorHashKeyWrapperBase.HashContext();
    VectorHashKeyWrapperGeneralBatchStorage small =
        new VectorHashKeyWrapperGeneralBatchStorage(context, 3, 2, 2, 2, 2, 2, 2, 12);
    VectorHashKeyWrapperGeneralBatchStorage large =
        new VectorHashKeyWrapperGeneralBatchStorage(context, 7, 2, 2, 2, 2, 2, 2, 12);
    VectorHashKeyWrapperGeneral expected = new VectorHashKeyWrapperGeneral(
        context, 2, 2, 2, 2, 2, 2, 12);
    VectorHashKeyWrapperBase first = small.rowViews[1];
    VectorHashKeyWrapperBase second = large.rowViews[5];
    for (VectorHashKeyWrapperBase key : new VectorHashKeyWrapperBase[] {first, second, expected}) {
      for (int index = 0; index < 2; ++index) {
        key.assignLong(index, index, 100 + index);
        key.assignDouble(index, 10.5 + index);
        key.assignString(index, new byte[] {1, 2, (byte) index, 4}, 1, 2);
        key.assignDecimal(index, new HiveDecimalWritable(
            HiveDecimal.create(index + ".125")));
        key.assignTimestamp(index, new Timestamp(12345 + index));
        key.assignIntervalDayTime(index, new HiveIntervalDayTime(123 + index, 7 + index));
      }
      key.setHashKey();
    }
    assertEquivalent(first, second);
    assertEquivalent(expected, first);
    assertEquivalent(expected, second);
    for (int type = 0; type < 6; ++type) {
      switch (type) {
      case 0:
        second.assignLong(1, 1, 999);
        break;
      case 1:
        second.assignDouble(1, 999);
        break;
      case 2:
        second.assignString(1, new byte[] {9, 9}, 0, 2);
        break;
      case 3:
        second.assignDecimal(1, new HiveDecimalWritable(HiveDecimal.ZERO));
        break;
      case 4:
        second.assignTimestamp(1, new Timestamp(999));
        break;
      case 5:
        second.assignIntervalDayTime(1, new HiveIntervalDayTime(999, 0));
        break;
      default:
        throw new AssertionError(type);
      }
      // Keep the original cached hash to test comparison of every value type.
      assertFalse(first.equals(second));
      assertFalse(second.equals(first));
      assertFalse(expected.equals(second));
      assertFalse(second.equals(expected));
      second.assignLong(1, 1, 101);
      second.assignDouble(1, 11.5);
      second.assignString(1, new byte[] {1, 2, 1, 4}, 1, 2);
      second.assignDecimal(1, new HiveDecimalWritable(HiveDecimal.create("1.125")));
      second.assignTimestamp(1, new Timestamp(12346));
      second.assignIntervalDayTime(1, new HiveIntervalDayTime(124, 8));
    }
    for (VectorHashKeyWrapperBase key : new VectorHashKeyWrapperBase[] {first, second, expected}) {
      key.assignNullLong(1, 1);
      key.assignNullDouble(3, 1);
      key.assignNullString(5, 1);
      key.assignNullDecimal(7, 1);
      key.assignNullTimestamp(9, 1);
      key.assignNullIntervalDayTime(11, 1);
      key.setHashKey();
    }
    assertEquivalent(first, second);
    assertEquivalent(expected, first);
    assertEquivalent(expected, (VectorHashKeyWrapperBase) second.copyKey());

    VectorHashKeyWrapperGeneralBatchStorage differentLayout =
        new VectorHashKeyWrapperGeneralBatchStorage(context, 3, 3, 1, 2, 2, 2, 2, 12);
    // Force matching cached hashes so shape validation itself is exercised.
    differentLayout.rowViews[1].hashcode = first.hashcode;
    assertFalse(first.equals(differentLayout.rowViews[1]));
    assertFalse(differentLayout.rowViews[1].equals(first));
    assertFalse(expected.equals(differentLayout.rowViews[1]));
    assertFalse(differentLayout.rowViews[1].equals(expected));
  }

  private static VectorHashKeyWrapperBatch compile() throws HiveException {
    VectorExpression[] expressions = new VectorExpression[TYPES.length];
    for (int key = 0; key < TYPES.length; ++key) {
      expressions[key] = new IdentityExpression(key);
    }
    return VectorHashKeyWrapperBatch.compileKeyWrapperBatch(expressions, TYPES);
  }

  private static VectorizedRowBatch batch(boolean selected, boolean repeating, boolean nullable) {
    VectorizedRowBatch batch = new VectorizedRowBatch(TYPES.length);
    batch.cols[0] = new BytesColumnVector();
    batch.cols[1] = new DecimalColumnVector(20, 4);
    batch.cols[2] = new DoubleColumnVector();
    batch.cols[3] = new TimestampColumnVector();
    batch.cols[4] = new LongColumnVector();
    batch.cols[5] = new IntervalDayTimeColumnVector();
    batch.cols[6] = new BytesColumnVector();
    batch.cols[7] = new LongColumnVector();
    batch.cols[8] = new DoubleColumnVector();
    for (int row = 0; row < 7; ++row) {
      byte[] string = ("__row-" + row + "__").getBytes(StandardCharsets.UTF_8);
      ((BytesColumnVector) batch.cols[0]).setRef(row, string, 2, string.length - 4);
      ((DecimalColumnVector) batch.cols[1]).set(row, HiveDecimal.create(row + ".1250"));
      ((DoubleColumnVector) batch.cols[2]).vector[row] = row + 0.25;
      Timestamp timestamp = new Timestamp(1000L * row);
      timestamp.setNanos(123456789 + row);
      ((TimestampColumnVector) batch.cols[3]).set(row, timestamp);
      ((LongColumnVector) batch.cols[4]).vector[row] = 100L + row;
      ((IntervalDayTimeColumnVector) batch.cols[5]).set(row, new HiveIntervalDayTime(row, 7 + row));
      ((BytesColumnVector) batch.cols[6]).setRef(row, string, 2, row % 2 == 0 ? 0 : string.length - 4);
      ((LongColumnVector) batch.cols[7]).vector[row] = -100L - row;
      ((DoubleColumnVector) batch.cols[8]).vector[row] = -row - 0.5;
    }
    for (int key = 0; key < TYPES.length; ++key) {
      ColumnVector column = batch.cols[key];
      column.noNulls = !nullable;
      column.isRepeating = repeating;
      if (nullable) {
        for (int row = 0; row < 7; ++row) {
          column.isNull[row] = (row + key) % 3 == 0;
        }
      }
    }
    batch.size = 3;
    batch.selectedInUse = selected;
    batch.selected[0] = 6;
    batch.selected[1] = 4;
    batch.selected[2] = 1;
    return batch;
  }

  private static void assertRows(VectorHashKeyWrapperBatch wrappers, VectorizedRowBatch batch,
      boolean[] overrides) {
    for (int denseRow = 0; denseRow < batch.size; ++denseRow) {
      int sourceRow = batch.selectedInUse ? batch.selected[denseRow] : denseRow;
      VectorHashKeyWrapperGeneral expected = expected(batch, sourceRow, overrides);
      VectorHashKeyWrapperBase view = wrappers.getVectorHashKeyWrappers()[denseRow];
      assertTrue(view instanceof VectorHashKeyWrapperGeneralRowView);
      assertEquivalent(expected, view);
      VectorHashKeyWrapperBase owned = (VectorHashKeyWrapperBase) view.copyKey();
      assertTrue(owned instanceof VectorHashKeyWrapperGeneral);
      assertEquivalent(expected, owned);
      assertEquivalent(view, owned);
      for (int key = 0; key < TYPES.length; ++key) {
        assertEquals(expected.isNull(key), view.isNull(key));
      }
    }
  }

  private static VectorHashKeyWrapperGeneral expected(VectorizedRowBatch batch, int row,
      boolean[] overrides) {
    VectorHashKeyWrapperGeneral expected = new VectorHashKeyWrapperGeneral(
        new VectorHashKeyWrapperBase.HashContext(), 2, 2, 2, 1, 1, 1, TYPES.length);
    int[] indices = {0, 0, 0, 0, 0, 0, 1, 1, 1};
    for (int key = 0; key < TYPES.length; ++key) {
      ColumnVector column = batch.cols[key];
      int inputRow = column.isRepeating ? 0 : row;
      int index = indices[key];
      boolean isNull = (overrides != null && overrides[key]) ||
          (!column.noNulls && column.isNull[inputRow]);
      switch (key) {
      case 0:
      case 6:
        BytesColumnVector bytes = (BytesColumnVector) column;
        if (isNull) {
          expected.assignNullString(key, index);
        } else {
          expected.assignString(index, bytes.vector[inputRow], bytes.start[inputRow], bytes.length[inputRow]);
        }
        break;
      case 1:
        if (isNull) {
          expected.assignNullDecimal(key, index);
        } else {
          expected.assignDecimal(index, ((DecimalColumnVector) column).vector[inputRow]);
        }
        break;
      case 2:
      case 8:
        if (isNull) {
          expected.assignNullDouble(key, index);
        } else {
          expected.assignDouble(index, ((DoubleColumnVector) column).vector[inputRow]);
        }
        break;
      case 3:
        if (isNull) {
          expected.assignNullTimestamp(key, index);
        } else {
          expected.assignTimestamp(index, (TimestampColumnVector) column, inputRow);
        }
        break;
      case 4:
      case 7:
        if (isNull) {
          expected.assignNullLong(key, index);
        } else {
          expected.assignLong(key, index, ((LongColumnVector) column).vector[inputRow]);
        }
        break;
      case 5:
        if (isNull) {
          expected.assignNullIntervalDayTime(key, index);
        } else {
          expected.assignIntervalDayTime(index, (IntervalDayTimeColumnVector) column, inputRow);
        }
        break;
      default:
        throw new AssertionError(key);
      }
    }
    expected.setHashKey();
    return expected;
  }

  private static void assertEquivalent(VectorHashKeyWrapperBase first, VectorHashKeyWrapperBase second) {
    assertEquals(first.hashCode(), second.hashCode());
    assertTrue(first.equals(second));
    assertTrue(second.equals(first));
  }
}
