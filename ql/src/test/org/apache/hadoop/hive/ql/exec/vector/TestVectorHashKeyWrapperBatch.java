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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneral;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperThreeLong;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Unit test for VectorHashKeyWrapperBatch class.
 */
public class TestVectorHashKeyWrapperBatch {

  // Specific test for HIVE-18744 --
  // Tests Timestamp assignment.
  @Test
  public void testVectorHashKeyWrapperBatch() throws HiveException {

    VectorExpression[] keyExpressions =
        new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos =
        new TypeInfo[] {TypeInfoFactory.timestampTypeInfo};
    VectorHashKeyWrapperBatch vhkwb =
        VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
            keyExpressions,
            typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.selectedInUse = false;
    batch.size = 10;
    TimestampColumnVector timestampColVector = new TimestampColumnVector(batch.DEFAULT_SIZE);;
    batch.cols[0] = timestampColVector;
    timestampColVector.reset();
    // Cause Timestamp object to be replaced (in buggy code) with ZERO_TIMESTAMP.
    timestampColVector.noNulls = false;
    timestampColVector.isNull[0] = true;
    Timestamp scratch = new Timestamp(2039);
    Timestamp ts0 = new Timestamp(2039);
    scratch.setTime(ts0.getTime());
    scratch.setNanos(ts0.getNanos());
    timestampColVector.set(1, scratch);
    Timestamp ts1 = new Timestamp(33222);
    scratch.setTime(ts1.getTime());
    scratch.setNanos(ts1.getNanos());
    timestampColVector.set(2, scratch);
    batch.size = 3;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    VectorHashKeyWrapperBase vhk = vhkwArray[0];
    assertTrue(vhk.isNull(0));
    vhk = vhkwArray[1];
    assertFalse(vhk.isNull(0));
    assertEquals(vhk.getTimestamp(0), ts0);
    vhk = vhkwArray[2];
    assertFalse(vhk.isNull(0));
    assertEquals(vhk.getTimestamp(0), ts1);
  }

  // Test for HIVE-24575
  @Test
  public void testVectorHashKeyWrapperSingleStringCopyKey() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.selectedInUse = false;
    BytesColumnVector bytesColumnVector = new BytesColumnVector();
    bytesColumnVector.initBuffer(1024);
    batch.cols[0] = bytesColumnVector;

    byte[] contents = "education_reference".getBytes();
    bytesColumnVector.setVal(0, "system_management".getBytes());
    bytesColumnVector.setVal(1, "travel_transportation".getBytes());
    bytesColumnVector.setVal(2, contents);
    bytesColumnVector.setVal(3, "app_management".getBytes());
    batch.size = 4;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 1);
    }
    VectorHashKeyWrapperBase hashKey2 = vhkwArray[2];
    VectorHashKeyWrapperBase hashKey1 = vhkwArray[1];

    assertTrue(StringExpr.equal(hashKey2.getBytes(0), hashKey2.getByteStart(0), hashKey2.getByteLength(0),
            contents, 0, contents.length));
    assertFalse(StringExpr.equal(hashKey2.getBytes(0), hashKey2.getByteStart(0), hashKey2.getByteLength(0),
        hashKey1.getBytes(0), hashKey1.getByteStart(0), hashKey1.getByteLength(0)));

    hashKey2.copyKey(hashKey1);

    assertTrue(StringExpr.equal(hashKey2.getBytes(0), hashKey2.getByteStart(0), hashKey2.getByteLength(0),
            contents, 0, contents.length));
    assertTrue(StringExpr.equal(hashKey2.getBytes(0), hashKey2.getByteStart(0), hashKey2.getByteLength(0),
        hashKey1.getBytes(0), hashKey1.getByteStart(0), hashKey1.getByteLength(0)));
    assertStringWrapperConsistentWithGeneral(hashKey1, vhkwb, 1);
  }


  @Test
  public void testVectorHashKeyWrapperSingleString() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.selectedInUse = false;
    BytesColumnVector bytesColumnVector = new BytesColumnVector();
    bytesColumnVector.initBuffer(1024);
    batch.cols[0] = bytesColumnVector;

    byte[] alpha = "alpha".getBytes();
    byte[] beta = "beta".getBytes();
    bytesColumnVector.setVal(0, alpha);
    bytesColumnVector.setVal(1, beta);
    bytesColumnVector.setVal(2, alpha);
    batch.size = 3;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    assertTrue(vhkwArray[0] instanceof VectorHashKeyWrapperSingleString);
    assertTrue(vhkwArray[1] instanceof VectorHashKeyWrapperSingleString);
    assertTrue(vhkwArray[2] instanceof VectorHashKeyWrapperSingleString);
    assertEquals(vhkwArray[0], vhkwArray[2]);
    assertEquals(vhkwArray[0].hashCode(), vhkwArray[2].hashCode());
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    for (int i = 0; i < batch.size; i++) {
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 1);
    }

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperSingleString);
    assertEquals(vhkwArray[0], copy);
    assertEquals(vhkwArray[0].hashCode(), copy.hashCode());
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 1);

    bytesColumnVector.vector[0][0] = 'z';
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(0), vhkwArray[0].getByteStart(0),
        vhkwArray[0].getByteLength(0), copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0)));

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        beta, 0, beta.length));
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 1);
  }

  @Test
  public void testVectorHashKeyWrapperSingleStringNull() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.selectedInUse = false;
    BytesColumnVector bytesColumnVector = new BytesColumnVector();
    bytesColumnVector.initBuffer(1024);
    bytesColumnVector.noNulls = false;
    bytesColumnVector.setVal(0, "not-null".getBytes());
    bytesColumnVector.isNull[1] = true;
    bytesColumnVector.setVal(2, "not-null".getBytes());
    bytesColumnVector.isNull[3] = true;
    batch.cols[0] = bytesColumnVector;
    batch.size = 4;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    assertTrue(vhkwArray[0] instanceof VectorHashKeyWrapperSingleString);
    assertFalse(vhkwArray[0].isNull(0));
    assertTrue(vhkwArray[1].isNull(0));
    assertFalse(vhkwArray[2].isNull(0));
    assertTrue(vhkwArray[3].isNull(0));
    assertEquals(vhkwArray[0], vhkwArray[2]);
    assertEquals(vhkwArray[1], vhkwArray[3]);
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    for (int i = 0; i < batch.size; i++) {
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 1);
    }
  }

  @Test
  public void testVectorHashKeyWrapperTwoString() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    batch.selectedInUse = false;
    BytesColumnVector firstColumnVector = new BytesColumnVector();
    firstColumnVector.initBuffer(1024);
    BytesColumnVector secondColumnVector = new BytesColumnVector();
    secondColumnVector.initBuffer(1024);
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;

    byte[] alpha = "alpha".getBytes();
    byte[] beta = "beta".getBytes();
    byte[] one = "one".getBytes();
    byte[] two = "two".getBytes();
    firstColumnVector.setVal(0, alpha);
    secondColumnVector.setVal(0, one);
    firstColumnVector.setVal(1, alpha);
    secondColumnVector.setVal(1, two);
    firstColumnVector.setVal(2, alpha);
    secondColumnVector.setVal(2, one);
    firstColumnVector.setVal(3, beta);
    secondColumnVector.setVal(3, one);
    batch.size = 4;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperTwoString);
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 2);
    }
    assertEquals(vhkwArray[0], vhkwArray[2]);
    assertEquals(vhkwArray[0].hashCode(), vhkwArray[2].hashCode());
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    assertFalse(vhkwArray[0].equals(vhkwArray[3]));

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperTwoString);
    assertEquals(vhkwArray[0], copy);
    assertEquals(vhkwArray[0].hashCode(), copy.hashCode());
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 2);

    firstColumnVector.vector[0][0] = 'z';
    secondColumnVector.vector[0][0] = 'z';
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertTrue(StringExpr.equal(copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1),
        one, 0, one.length));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(0), vhkwArray[0].getByteStart(0),
        vhkwArray[0].getByteLength(0), copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0)));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(1), vhkwArray[0].getByteStart(1),
        vhkwArray[0].getByteLength(1), copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1)));

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertTrue(StringExpr.equal(copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1),
        two, 0, two.length));
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 2);
  }

  @Test
  public void testVectorHashKeyWrapperTwoStringNull() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    batch.selectedInUse = false;
    BytesColumnVector firstColumnVector = new BytesColumnVector();
    firstColumnVector.initBuffer(1024);
    firstColumnVector.noNulls = false;
    BytesColumnVector secondColumnVector = new BytesColumnVector();
    secondColumnVector.initBuffer(1024);
    secondColumnVector.noNulls = false;
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;

    firstColumnVector.setVal(0, "left".getBytes());
    secondColumnVector.setVal(0, "right".getBytes());
    firstColumnVector.isNull[1] = true;
    secondColumnVector.setVal(1, "right".getBytes());
    firstColumnVector.setVal(2, "left".getBytes());
    secondColumnVector.isNull[2] = true;
    firstColumnVector.isNull[3] = true;
    secondColumnVector.isNull[3] = true;
    firstColumnVector.isNull[4] = true;
    secondColumnVector.setVal(4, "right".getBytes());
    batch.size = 5;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperTwoString);
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 2);
    }
    assertFalse(vhkwArray[0].isNull(0));
    assertFalse(vhkwArray[0].isNull(1));
    assertTrue(vhkwArray[1].isNull(0));
    assertFalse(vhkwArray[1].isNull(1));
    assertFalse(vhkwArray[2].isNull(0));
    assertTrue(vhkwArray[2].isNull(1));
    assertTrue(vhkwArray[3].isNull(0));
    assertTrue(vhkwArray[3].isNull(1));
    assertEquals(vhkwArray[1], vhkwArray[4]);
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    assertFalse(vhkwArray[0].equals(vhkwArray[2]));
    assertFalse(vhkwArray[0].equals(vhkwArray[3]));
  }

  @Test
  public void testVectorHashKeyWrapperThreeLong() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1), new IdentityExpression(2) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    batch.selectedInUse = false;
    LongColumnVector firstColumnVector = new LongColumnVector();
    LongColumnVector secondColumnVector = new LongColumnVector();
    LongColumnVector thirdColumnVector = new LongColumnVector();
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;
    batch.cols[2] = thirdColumnVector;

    firstColumnVector.vector[0] = 10;
    secondColumnVector.vector[0] = 20;
    thirdColumnVector.vector[0] = 30;
    firstColumnVector.vector[1] = 10;
    secondColumnVector.vector[1] = 20;
    thirdColumnVector.vector[1] = 31;
    firstColumnVector.vector[2] = 10;
    secondColumnVector.vector[2] = 20;
    thirdColumnVector.vector[2] = 30;
    firstColumnVector.vector[3] = 11;
    secondColumnVector.vector[3] = 20;
    thirdColumnVector.vector[3] = 30;
    batch.size = 4;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperThreeLong);
      assertLongWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 3);
    }
    assertEquals(vhkwArray[0], vhkwArray[2]);
    assertEquals(vhkwArray[0].hashCode(), vhkwArray[2].hashCode());
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    assertFalse(vhkwArray[0].equals(vhkwArray[3]));

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperThreeLong);
    assertEquals(vhkwArray[0], copy);
    assertEquals(vhkwArray[0].hashCode(), copy.hashCode());
    assertLongWrapperConsistentWithGeneral(copy, vhkwb, 3);

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertEquals(10, copy.getLongValue(0));
    assertEquals(20, copy.getLongValue(1));
    assertEquals(31, copy.getLongValue(2));
    assertLongWrapperConsistentWithGeneral(copy, vhkwb, 3);
  }

  @Test
  public void testVectorHashKeyWrapperThreeLongNull() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1), new IdentityExpression(2) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    batch.selectedInUse = false;
    LongColumnVector firstColumnVector = new LongColumnVector();
    firstColumnVector.noNulls = false;
    LongColumnVector secondColumnVector = new LongColumnVector();
    secondColumnVector.noNulls = false;
    LongColumnVector thirdColumnVector = new LongColumnVector();
    thirdColumnVector.noNulls = false;
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;
    batch.cols[2] = thirdColumnVector;

    firstColumnVector.vector[0] = 10;
    secondColumnVector.vector[0] = 20;
    thirdColumnVector.vector[0] = 30;
    firstColumnVector.isNull[1] = true;
    secondColumnVector.vector[1] = 20;
    thirdColumnVector.vector[1] = 30;
    firstColumnVector.vector[2] = 10;
    secondColumnVector.isNull[2] = true;
    thirdColumnVector.vector[2] = 30;
    firstColumnVector.vector[3] = 10;
    secondColumnVector.vector[3] = 20;
    thirdColumnVector.isNull[3] = true;
    firstColumnVector.isNull[4] = true;
    secondColumnVector.vector[4] = 20;
    thirdColumnVector.vector[4] = 30;
    firstColumnVector.isNull[5] = true;
    secondColumnVector.isNull[5] = true;
    thirdColumnVector.isNull[5] = true;
    batch.size = 6;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperThreeLong);
      assertLongWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 3);
    }
    assertFalse(vhkwArray[0].isNull(0));
    assertFalse(vhkwArray[0].isNull(1));
    assertFalse(vhkwArray[0].isNull(2));
    assertTrue(vhkwArray[1].isNull(0));
    assertFalse(vhkwArray[1].isNull(1));
    assertFalse(vhkwArray[1].isNull(2));
    assertFalse(vhkwArray[2].isNull(0));
    assertTrue(vhkwArray[2].isNull(1));
    assertFalse(vhkwArray[2].isNull(2));
    assertFalse(vhkwArray[3].isNull(0));
    assertFalse(vhkwArray[3].isNull(1));
    assertTrue(vhkwArray[3].isNull(2));
    assertEquals(vhkwArray[1], vhkwArray[4]);
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    assertFalse(vhkwArray[0].equals(vhkwArray[2]));
    assertFalse(vhkwArray[0].equals(vhkwArray[3]));
    assertFalse(vhkwArray[0].equals(vhkwArray[5]));
  }

  private void assertLongWrapperConsistentWithGeneral(
      VectorHashKeyWrapperBase longWrapper, VectorHashKeyWrapperBatch vhkwb, int longCount) {
    VectorHashKeyWrapperGeneral generalWrapper = new VectorHashKeyWrapperGeneral(
        new VectorHashKeyWrapperBase.HashContext(), longCount, 0, 0, 0, 0, 0, longCount);
    for (int i = 0; i < longCount; i++) {
      if (longWrapper.isNull(i)) {
        generalWrapper.assignNullLong(i, i);
      } else {
        generalWrapper.assignLong(i, i, longWrapper.getLongValue(i));
      }
    }
    generalWrapper.setHashKey();

    assertEquals(generalWrapper.hashCode(), longWrapper.hashCode());
    for (int i = 0; i < longCount; i++) {
      assertEquals(generalWrapper.isNull(i), longWrapper.isNull(i));
      assertEquals(generalWrapper.getLongValue(i), longWrapper.getLongValue(i));
    }
    assertEquals(generalWrapper.stringifyKeys(vhkwb), longWrapper.stringifyKeys(vhkwb));
    assertEquals(generalWrapper.toString(), longWrapper.toString());
    assertEquals(generalWrapper.getVariableSize(), longWrapper.getVariableSize());
  }

  private void assertStringWrapperConsistentWithGeneral(
      VectorHashKeyWrapperBase stringWrapper, VectorHashKeyWrapperBatch vhkwb, int stringCount) {
    VectorHashKeyWrapperGeneral generalWrapper = new VectorHashKeyWrapperGeneral(
        new VectorHashKeyWrapperBase.HashContext(), 0, 0, stringCount, 0, 0, 0, stringCount);
    for (int i = 0; i < stringCount; i++) {
      if (stringWrapper.isNull(i)) {
        generalWrapper.assignNullString(i, i);
      } else {
        generalWrapper.assignString(i, stringWrapper.getBytes(i), stringWrapper.getByteStart(i),
            stringWrapper.getByteLength(i));
      }
    }
    generalWrapper.setHashKey();

    assertEquals(generalWrapper.hashCode(), stringWrapper.hashCode());
    for (int i = 0; i < stringCount; i++) {
      assertEquals(generalWrapper.isNull(i), stringWrapper.isNull(i));
      assertEquals(generalWrapper.getByteStart(i), stringWrapper.getByteStart(i));
      assertEquals(generalWrapper.getByteLength(i), stringWrapper.getByteLength(i));
      if (generalWrapper.isNull(i)) {
        assertEquals(generalWrapper.getBytes(i), stringWrapper.getBytes(i));
      } else {
        assertTrue(StringExpr.equal(generalWrapper.getBytes(i), generalWrapper.getByteStart(i),
            generalWrapper.getByteLength(i), stringWrapper.getBytes(i), stringWrapper.getByteStart(i),
            stringWrapper.getByteLength(i)));
      }
    }
    assertEquals(generalWrapper.stringifyKeys(vhkwb), stringWrapper.stringifyKeys(vhkwb));
    assertEquals(generalWrapper.toString(), stringWrapper.toString());
    assertEquals(generalWrapper.getVariableSize(), stringWrapper.getVariableSize());
  }

}
