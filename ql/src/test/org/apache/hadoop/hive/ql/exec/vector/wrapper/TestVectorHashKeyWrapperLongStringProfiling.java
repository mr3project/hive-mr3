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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;
import org.slf4j.Logger;

public class TestVectorHashKeyWrapperLongStringProfiling {

  private static final String FORMAT = "yyyyy {} {} {} {} {} {} {} {} - {} {} {} {} {} {} {} {}";
  private static final TypeInfo[] TYPES = {
      TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
      TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo};

  @Test
  public void testAssignmentGroupsAcrossBatchVariantsAndGroupingSets() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = compile(TYPES);
    for (boolean selected : new boolean[] {false, true}) {
      for (boolean repeating : new boolean[] {false, true}) {
        for (boolean nullable : new boolean[] {false, true}) {
          wrappers.evaluateBatch(batch(selected, repeating, nullable));
        }
      }
    }
    boolean[] allNull = new boolean[TYPES.length];
    Arrays.fill(allNull, true);
    wrappers.evaluateBatchGroupingSets(batch(true, false, false), allNull);

    Logger log = mock(Logger.class);
    wrappers.logMethodCallCounts(log);
    wrappers.logMethodCallCounts(log);
    // Per column: one bulk copy, five repeating assignments, and three other assignments.
    verify(log).error(FORMAT,
        1L, 9L, 3L, 15L, 9L, 2L, 10L, 6L, 27L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testRowOperationCounts() throws HiveException {
    VectorHashKeyWrapperBatch wrappers = compile(TYPES);
    wrappers.evaluateBatch(batch(false, false, false));
    VectorHashKeyWrapperLongStringRowView view =
        (VectorHashKeyWrapperLongStringRowView) wrappers.getVectorHashKeyWrappers()[0];
    VectorHashKeyWrapperGeneralLongString retained =
        (VectorHashKeyWrapperGeneralLongString) view.copyKey();
    assertTrue(view.equals(view));
    assertTrue(retained.equals(view));
    assertTrue(view.equals(retained));
    view.copyKey(retained);
    view.setHashKey();
    view.clearIsNull();
    view.setNull();
    view.getVariableSize();

    Logger log = mock(Logger.class);
    wrappers.logMethodCallCounts(log);
    verify(log).error(FORMAT,
        1L, 1L, 3L, 0L, 0L, 2L, 0L, 0L, 4L, 1L, 2L, 1L, 1L, 1L, 1L, 1L);
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testConcurrentProfilersAndIndependentRoles() throws HiveException {
    VectorHashKeyWrapperBatch longString = compile(TYPES);
    VectorHashKeyWrapperBatch otherRole = compile(TYPES);
    VectorHashKeyWrapperBatch general = compile(new TypeInfo[] {TypeInfoFactory.doubleTypeInfo});
    longString.evaluateBatch(batch(false, false, false));
    VectorizedRowBatch doubles = new VectorizedRowBatch(1);
    doubles.cols[0] = new DoubleColumnVector();
    doubles.size = 2;
    general.evaluateBatch(doubles);

    Logger log = mock(Logger.class);
    longString.logMethodCallCounts(log);
    general.logMethodCallCounts(log);
    otherRole.logMethodCallCounts(log);
    longString.logMethodCallCounts(log);
    general.logMethodCallCounts(log);
    otherRole.logMethodCallCounts(log);
    compile(new TypeInfo[0]).logMethodCallCounts(log);
    compile(new TypeInfo[] {TypeInfoFactory.longTypeInfo}).logMethodCallCounts(log);

    verify(log).error(FORMAT,
        1L, 1L, 3L, 0L, 0L, 2L, 0L, 0L, 3L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    verify(log).error("xxxxx {} {} {} {} {} {} {} {} - {} {} {} {} {} {} {} {}",
        1L, 1L, 0L, 1L, 0L, 0L, 0L, 0L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    verify(log).error(FORMAT,
        1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    verifyNoMoreInteractions(log);
  }

  private static VectorHashKeyWrapperBatch compile(TypeInfo[] types) throws HiveException {
    VectorExpression[] expressions = new VectorExpression[types.length];
    for (int key = 0; key < types.length; ++key) {
      expressions[key] = new IdentityExpression(key);
    }
    return VectorHashKeyWrapperBatch.compileKeyWrapperBatch(expressions, types);
  }

  private static VectorizedRowBatch batch(boolean selected, boolean repeating, boolean nullable) {
    VectorizedRowBatch batch = new VectorizedRowBatch(TYPES.length);
    for (int key = 0; key < TYPES.length; ++key) {
      ColumnVector column = key < 3 ? new LongColumnVector() : new BytesColumnVector();
      batch.cols[key] = column;
      column.noNulls = !nullable;
      column.isRepeating = repeating;
      for (int row = 0; row < 7; ++row) {
        column.isNull[row] = nullable && row % 2 == 0;
        if (key < 3) {
          ((LongColumnVector) column).vector[row] = 100L * key + row;
        } else {
          byte[] bytes = ("key-" + key + "-row-" + row).getBytes(StandardCharsets.UTF_8);
          ((BytesColumnVector) column).setRef(row, bytes, 0, bytes.length);
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
}
