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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneralLongString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLong;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLongSingleString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLongTwoString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperThreeLong;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperThreeString;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoLong;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoLongSingleString;
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
  public void testVectorHashKeyWrapperThreeString() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1), new IdentityExpression(2) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    batch.selectedInUse = false;
    BytesColumnVector firstColumnVector = new BytesColumnVector();
    firstColumnVector.initBuffer(1024);
    BytesColumnVector secondColumnVector = new BytesColumnVector();
    secondColumnVector.initBuffer(1024);
    BytesColumnVector thirdColumnVector = new BytesColumnVector();
    thirdColumnVector.initBuffer(1024);
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;
    batch.cols[2] = thirdColumnVector;

    byte[] alpha = "alpha".getBytes();
    byte[] beta = "beta".getBytes();
    byte[] one = "one".getBytes();
    byte[] two = "two".getBytes();
    byte[] red = "red".getBytes();
    byte[] blue = "blue".getBytes();
    firstColumnVector.setVal(0, alpha);
    secondColumnVector.setVal(0, one);
    thirdColumnVector.setVal(0, red);
    firstColumnVector.setVal(1, alpha);
    secondColumnVector.setVal(1, two);
    thirdColumnVector.setVal(1, red);
    firstColumnVector.setVal(2, alpha);
    secondColumnVector.setVal(2, one);
    thirdColumnVector.setVal(2, red);
    firstColumnVector.setVal(3, beta);
    secondColumnVector.setVal(3, one);
    thirdColumnVector.setVal(3, red);
    firstColumnVector.setVal(4, alpha);
    secondColumnVector.setVal(4, one);
    thirdColumnVector.setVal(4, blue);
    batch.size = 5;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperThreeString);
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 3);
    }
    assertEquals(vhkwArray[0], vhkwArray[2]);
    assertEquals(vhkwArray[0].hashCode(), vhkwArray[2].hashCode());
    assertFalse(vhkwArray[0].equals(vhkwArray[1]));
    assertFalse(vhkwArray[0].equals(vhkwArray[3]));
    assertFalse(vhkwArray[0].equals(vhkwArray[4]));

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperThreeString);
    assertEquals(vhkwArray[0], copy);
    assertEquals(vhkwArray[0].hashCode(), copy.hashCode());
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 3);

    firstColumnVector.vector[0][0] = 'z';
    secondColumnVector.vector[0][0] = 'z';
    thirdColumnVector.vector[0][0] = 'z';
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertTrue(StringExpr.equal(copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1),
        one, 0, one.length));
    assertTrue(StringExpr.equal(copy.getBytes(2), copy.getByteStart(2), copy.getByteLength(2),
        red, 0, red.length));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(0), vhkwArray[0].getByteStart(0),
        vhkwArray[0].getByteLength(0), copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0)));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(1), vhkwArray[0].getByteStart(1),
        vhkwArray[0].getByteLength(1), copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1)));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(2), vhkwArray[0].getByteStart(2),
        vhkwArray[0].getByteLength(2), copy.getBytes(2), copy.getByteStart(2), copy.getByteLength(2)));

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertTrue(StringExpr.equal(copy.getBytes(1), copy.getByteStart(1), copy.getByteLength(1),
        two, 0, two.length));
    assertTrue(StringExpr.equal(copy.getBytes(2), copy.getByteStart(2), copy.getByteLength(2),
        red, 0, red.length));
    assertStringWrapperConsistentWithGeneral(copy, vhkwb, 3);
  }

  @Test
  public void testVectorHashKeyWrapperThreeStringNull() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1), new IdentityExpression(2) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    batch.selectedInUse = false;
    BytesColumnVector firstColumnVector = new BytesColumnVector();
    firstColumnVector.initBuffer(1024);
    firstColumnVector.noNulls = false;
    BytesColumnVector secondColumnVector = new BytesColumnVector();
    secondColumnVector.initBuffer(1024);
    secondColumnVector.noNulls = false;
    BytesColumnVector thirdColumnVector = new BytesColumnVector();
    thirdColumnVector.initBuffer(1024);
    thirdColumnVector.noNulls = false;
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;
    batch.cols[2] = thirdColumnVector;

    firstColumnVector.setVal(0, "left".getBytes());
    secondColumnVector.setVal(0, "middle".getBytes());
    thirdColumnVector.setVal(0, "right".getBytes());
    firstColumnVector.isNull[1] = true;
    secondColumnVector.setVal(1, "middle".getBytes());
    thirdColumnVector.setVal(1, "right".getBytes());
    firstColumnVector.setVal(2, "left".getBytes());
    secondColumnVector.isNull[2] = true;
    thirdColumnVector.setVal(2, "right".getBytes());
    firstColumnVector.setVal(3, "left".getBytes());
    secondColumnVector.setVal(3, "middle".getBytes());
    thirdColumnVector.isNull[3] = true;
    firstColumnVector.isNull[4] = true;
    secondColumnVector.setVal(4, "middle".getBytes());
    thirdColumnVector.setVal(4, "right".getBytes());
    firstColumnVector.isNull[5] = true;
    secondColumnVector.isNull[5] = true;
    thirdColumnVector.isNull[5] = true;
    batch.size = 6;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(vhkwArray[i] instanceof VectorHashKeyWrapperThreeString);
      assertStringWrapperConsistentWithGeneral(vhkwArray[i], vhkwb, 3);
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

  @Test
  public void testVectorHashKeyWrapperSingleLongCopyKey() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.longTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    LongColumnVector longColumnVector = new LongColumnVector();
    batch.cols[0] = longColumnVector;
    longColumnVector.vector[0] = 10;
    longColumnVector.vector[1] = 20;
    batch.size = 2;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperSingleLong);
    assertEquals(vhkwArray[0], copy);

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertEquals(20, copy.getLongValue(0));
  }

  @Test
  public void testVectorHashKeyWrapperTwoLongCopyKey() throws HiveException {
    VectorExpression[] keyExpressions = new VectorExpression[] { new IdentityExpression(0),
        new IdentityExpression(1) };
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo};
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        keyExpressions,
        typeInfos);

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector firstColumnVector = new LongColumnVector();
    LongColumnVector secondColumnVector = new LongColumnVector();
    batch.cols[0] = firstColumnVector;
    batch.cols[1] = secondColumnVector;
    firstColumnVector.vector[0] = 10;
    secondColumnVector.vector[0] = 20;
    firstColumnVector.vector[1] = 30;
    secondColumnVector.vector[1] = 40;
    batch.size = 2;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] vhkwArray = vhkwb.getVectorHashKeyWrappers();
    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperTwoLong);
    assertEquals(vhkwArray[0], copy);

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertEquals(30, copy.getLongValue(0));
    assertEquals(40, copy.getLongValue(1));
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

  @Test
  public void testVectorHashKeyWrapperSingleLongSingleStringPermutations() throws HiveException {
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
  }

  @Test
  public void testVectorHashKeyWrapperSingleLongSingleStringNullPermutations() throws HiveException {
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
  }

  @Test
  public void testVectorHashKeyWrapperTwoLongSingleStringPermutations() throws HiveException {
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
  }

  @Test
  public void testVectorHashKeyWrapperTwoLongSingleStringNullPermutations() throws HiveException {
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
  }

  @Test
  public void testVectorHashKeyWrapperSingleLongTwoStringPermutations() throws HiveException {
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
  }

  @Test
  public void testVectorHashKeyWrapperSingleLongTwoStringNullPermutations() throws HiveException {
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertMixedLongStringNullWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
  }

  @Test
  public void testVectorHashKeyWrapperGeneralLongStringLongs() throws HiveException {
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperGeneralLongString.class, 4, 0);
  }

  @Test
  public void testVectorHashKeyWrapperGeneralLongStringStrings() throws HiveException {
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperGeneralLongString.class, 0, 4);
  }

  @Test
  public void testVectorHashKeyWrapperGeneralLongStringMixedPermutations() throws HiveException {
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperGeneralLongString.class, 2, 2);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperGeneralLongString.class, 2, 2);
    assertMixedLongStringWrapper(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperGeneralLongString.class, 2, 2);
  }

  @Test
  public void testSpecializedWrappersSelectedInUse() throws HiveException {
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleString.class, 0, 1);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperTwoString.class, 0, 2);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperThreeString.class, 0, 3);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperThreeLong.class, 3, 0);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertSpecializedWrapperSelectedInUse(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
  }

  @Test
  public void testSpecializedWrappersGroupingSetOverrides() throws HiveException {
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleString.class, 0, 1);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperTwoString.class, 0, 2);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperThreeString.class, 0, 3);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperThreeLong.class, 3, 0);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertSpecializedWrapperGroupingSetOverrides(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
  }

  @Test
  public void testSpecializedWrappersClearNullsBetweenEvaluations() throws HiveException {
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleString.class, 0, 1);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperTwoString.class, 0, 2);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo}, VectorHashKeyWrapperThreeString.class, 0, 3);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperThreeLong.class, 3, 0);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo}, VectorHashKeyWrapperSingleLongSingleString.class, 1, 1);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo},
        VectorHashKeyWrapperSingleLongTwoString.class, 1, 2);
    assertSpecializedWrapperClearsNullsBetweenEvaluations(new TypeInfo[] {TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo},
        VectorHashKeyWrapperTwoLongSingleString.class, 2, 1);
  }

  private void assertMixedLongStringWrapper(TypeInfo[] typeInfos,
      Class<? extends VectorHashKeyWrapperBase> expectedWrapperClass, int longCount, int stringCount)
      throws HiveException {
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        identityExpressions(typeInfos.length), typeInfos);
    VectorizedRowBatch batch = createMixedLongStringBatch(typeInfos, false);

    long[][] longValues = longCount == 2 ?
        new long[][] {{10, 20}, {10, 20}, {10, 20}, {11, 20}} :
        new long[][] {{10}, {10}, {10}, {11}};
    String[][] stringValues = stringCount == 2 ?
        new String[][] {{"alpha", "one"}, {"alpha", "two"}, {"alpha", "one"}, {"alpha", "one"}} :
        new String[][] {{"alpha"}, {"beta"}, {"alpha"}, {"alpha"}};
    fillMixedLongStringBatch(batch, typeInfos, longValues, stringValues, null);

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] wrappers = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(expectedWrapperClass.isInstance(wrappers[i]));
      assertMixedLongStringWrapperConsistentWithGeneral(wrappers[i], vhkwb, longCount, stringCount);
    }
    assertEquals(wrappers[0], wrappers[2]);
    assertEquals(wrappers[0].hashCode(), wrappers[2].hashCode());
    assertFalse(wrappers[0].equals(wrappers[1]));
    assertFalse(wrappers[0].equals(wrappers[3]));

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) wrappers[0].copyKey();
    assertTrue(expectedWrapperClass.isInstance(copy));
    assertEquals(wrappers[0], copy);
    assertEquals(wrappers[0].hashCode(), copy.hashCode());
    assertMixedLongStringWrapperConsistentWithGeneral(copy, vhkwb, longCount, stringCount);

    mutateFirstStringValue(batch, typeInfos);
    for (int i = 0; i < stringCount; i++) {
      assertTrue(StringExpr.equal(copy.getBytes(i), copy.getByteStart(i), copy.getByteLength(i),
          stringValues[0][i].getBytes(), 0, stringValues[0][i].getBytes().length));
      assertFalse(StringExpr.equal(wrappers[0].getBytes(i), wrappers[0].getByteStart(i),
          wrappers[0].getByteLength(i), copy.getBytes(i), copy.getByteStart(i), copy.getByteLength(i)));
    }

    wrappers[1].copyKey(copy);
    assertEquals(wrappers[1], copy);
    assertMixedLongStringWrapperConsistentWithGeneral(copy, vhkwb, longCount, stringCount);
  }

  private void assertMixedLongStringNullWrapper(TypeInfo[] typeInfos,
      Class<? extends VectorHashKeyWrapperBase> expectedWrapperClass, int longCount, int stringCount)
      throws HiveException {
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        identityExpressions(typeInfos.length), typeInfos);
    VectorizedRowBatch batch = createMixedLongStringBatch(typeInfos, true);

    boolean twoKeyShape = typeInfos.length == 2;
    long[][] longValues = longCount == 2 ?
        new long[][] {{10, 20}, {10, 20}, {10, 20}, {10, 20}, {10, 20}, {10, 20}} :
        (twoKeyShape ? new long[][] {{10}, {10}, {10}, {10}, {10}} :
            new long[][] {{10}, {10}, {10}, {10}, {10}, {10}});
    String[][] stringValues = stringCount == 2 ?
        new String[][] {{"alpha", "one"}, {"alpha", "one"}, {"alpha", "one"}, {"alpha", "one"},
            {"alpha", "one"}, {"alpha", "one"}} :
        (twoKeyShape ? new String[][] {{"alpha"}, {"alpha"}, {"alpha"}, {"alpha"}, {"alpha"}} :
            new String[][] {{"alpha"}, {"alpha"}, {"alpha"}, {"alpha"}, {"alpha"}, {"alpha"}});
    boolean[][] isNull = twoKeyShape ?
        new boolean[][] {
            {false, false},
            {true, false},
            {false, true},
            {true, false},
            {true, true}} :
        new boolean[][] {
            {false, false, false},
            {true, false, false},
            {false, true, false},
            {false, false, true},
            {true, false, false},
            {true, true, true}};
    fillMixedLongStringBatch(batch, typeInfos, longValues, stringValues, isNull);

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] wrappers = vhkwb.getVectorHashKeyWrappers();
    for (int i = 0; i < batch.size; i++) {
      assertTrue(expectedWrapperClass.isInstance(wrappers[i]));
      assertMixedLongStringWrapperConsistentWithGeneral(wrappers[i], vhkwb, longCount, stringCount);
      for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
        assertEquals(isNull[i][keyIndex], wrappers[i].isNull(keyIndex));
      }
    }
    if (twoKeyShape) {
      assertEquals(wrappers[1], wrappers[3]);
      assertEquals(wrappers[1].hashCode(), wrappers[3].hashCode());
      assertFalse(wrappers[0].equals(wrappers[1]));
      assertFalse(wrappers[0].equals(wrappers[2]));
      assertFalse(wrappers[0].equals(wrappers[4]));
    } else {
      assertEquals(wrappers[1], wrappers[4]);
      assertEquals(wrappers[1].hashCode(), wrappers[4].hashCode());
      assertFalse(wrappers[0].equals(wrappers[1]));
      assertFalse(wrappers[0].equals(wrappers[2]));
      assertFalse(wrappers[0].equals(wrappers[3]));
      assertFalse(wrappers[0].equals(wrappers[5]));
    }
  }


  private void assertSpecializedWrapperSelectedInUse(TypeInfo[] typeInfos,
      Class<? extends VectorHashKeyWrapperBase> expectedWrapperClass, int longCount, int stringCount)
      throws HiveException {
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        identityExpressions(typeInfos.length), typeInfos);
    VectorizedRowBatch batch = createMixedLongStringBatch(typeInfos, false);
    fillSelectedInUseBatch(batch, typeInfos);
    batch.selectedInUse = true;
    batch.selected[0] = 4;
    batch.selected[1] = 2;
    batch.selected[2] = 0;
    batch.size = 3;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] wrappers = vhkwb.getVectorHashKeyWrappers();
    assertTrue(expectedWrapperClass.isInstance(wrappers[0]));
    assertTrue(expectedWrapperClass.isInstance(wrappers[1]));
    assertTrue(expectedWrapperClass.isInstance(wrappers[2]));
    assertEquals(wrappers[0], wrappers[2]);
    assertEquals(wrappers[0].hashCode(), wrappers[2].hashCode());
    assertFalse(wrappers[0].equals(wrappers[1]));
    for (int i = 0; i < batch.size; i++) {
      assertSpecializedWrapperConsistentWithGeneral(wrappers[i], vhkwb, longCount, stringCount);
    }
  }

  private void assertSpecializedWrapperGroupingSetOverrides(TypeInfo[] typeInfos,
      Class<? extends VectorHashKeyWrapperBase> expectedWrapperClass, int longCount, int stringCount)
      throws HiveException {
    for (int overrideKeyIndex = 0; overrideKeyIndex < typeInfos.length; overrideKeyIndex++) {
      VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
          identityExpressions(typeInfos.length), typeInfos);
      VectorizedRowBatch batch = createMixedLongStringBatch(typeInfos, false);
      fillSingleRowWithValues(batch, typeInfos, 0, 100, "grouping");
      batch.size = 1;
      boolean[] groupingSetsOverrideIsNulls = new boolean[typeInfos.length];
      groupingSetsOverrideIsNulls[overrideKeyIndex] = true;

      vhkwb.evaluateBatchGroupingSets(batch, groupingSetsOverrideIsNulls);
      VectorHashKeyWrapperBase wrapper = vhkwb.getVectorHashKeyWrappers()[0];
      assertTrue(expectedWrapperClass.isInstance(wrapper));
      for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
        assertEquals(groupingSetsOverrideIsNulls[keyIndex], wrapper.isNull(keyIndex));
      }
      assertSpecializedWrapperConsistentWithGeneral(wrapper, vhkwb, longCount, stringCount);
    }
  }

  private void assertSpecializedWrapperClearsNullsBetweenEvaluations(TypeInfo[] typeInfos,
      Class<? extends VectorHashKeyWrapperBase> expectedWrapperClass, int longCount, int stringCount)
      throws HiveException {
    VectorHashKeyWrapperBatch vhkwb = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(
        identityExpressions(typeInfos.length), typeInfos);
    VectorizedRowBatch batch = createMixedLongStringBatch(typeInfos, true);
    fillSingleRowWithValues(batch, typeInfos, 0, 200, "null-pass");
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      batch.cols[keyIndex].isNull[0] = true;
    }
    batch.size = 1;

    vhkwb.evaluateBatch(batch);
    VectorHashKeyWrapperBase wrapper = vhkwb.getVectorHashKeyWrappers()[0];
    assertTrue(expectedWrapperClass.isInstance(wrapper));
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      assertTrue(wrapper.isNull(keyIndex));
    }
    assertSpecializedWrapperConsistentWithGeneral(wrapper, vhkwb, longCount, stringCount);

    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      batch.cols[keyIndex].noNulls = true;
      batch.cols[keyIndex].isNull[0] = false;
    }
    fillSingleRowWithValues(batch, typeInfos, 0, 300, "not-null-pass");

    vhkwb.evaluateBatch(batch);
    wrapper = vhkwb.getVectorHashKeyWrappers()[0];
    assertTrue(expectedWrapperClass.isInstance(wrapper));
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      assertFalse(wrapper.isNull(keyIndex));
    }
    assertSpecializedWrapperConsistentWithGeneral(wrapper, vhkwb, longCount, stringCount);
  }

  private void fillSelectedInUseBatch(VectorizedRowBatch batch, TypeInfo[] typeInfos) {
    fillSingleRowWithValues(batch, typeInfos, 0, 10, "same");
    fillSingleRowWithValues(batch, typeInfos, 1, 20, "unselected-1");
    fillSingleRowWithValues(batch, typeInfos, 2, 30, "different");
    fillSingleRowWithValues(batch, typeInfos, 3, 40, "unselected-3");
    fillSingleRowWithValues(batch, typeInfos, 4, 10, "same");
  }

  private void fillSingleRowWithValues(VectorizedRowBatch batch, TypeInfo[] typeInfos, int row, long longBase,
      String stringPrefix) {
    int longIndex = 0;
    int stringIndex = 0;
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      if (typeInfos[keyIndex] == TypeInfoFactory.longTypeInfo) {
        ((LongColumnVector) batch.cols[keyIndex]).vector[row] = longBase + longIndex++;
      } else {
        byte[] value = (stringPrefix + "_" + stringIndex++).getBytes();
        ((BytesColumnVector) batch.cols[keyIndex]).setVal(row, value);
      }
    }
  }

  private void assertSpecializedWrapperConsistentWithGeneral(
      VectorHashKeyWrapperBase wrapper, VectorHashKeyWrapperBatch vhkwb, int longCount, int stringCount) {
    if (longCount == 0) {
      assertStringWrapperConsistentWithGeneral(wrapper, vhkwb, stringCount);
    } else if (stringCount == 0) {
      assertLongWrapperConsistentWithGeneral(wrapper, vhkwb, longCount);
    } else {
      assertMixedLongStringWrapperConsistentWithGeneral(wrapper, vhkwb, longCount, stringCount);
    }
  }

  private VectorExpression[] identityExpressions(int count) {
    VectorExpression[] keyExpressions = new VectorExpression[count];
    for (int i = 0; i < count; i++) {
      keyExpressions[i] = new IdentityExpression(i);
    }
    return keyExpressions;
  }

  private VectorizedRowBatch createMixedLongStringBatch(TypeInfo[] typeInfos, boolean mayHaveNulls) {
    VectorizedRowBatch batch = new VectorizedRowBatch(typeInfos.length);
    batch.selectedInUse = false;
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      if (typeInfos[keyIndex] == TypeInfoFactory.longTypeInfo) {
        LongColumnVector longColumnVector = new LongColumnVector();
        longColumnVector.noNulls = !mayHaveNulls;
        batch.cols[keyIndex] = longColumnVector;
      } else {
        BytesColumnVector bytesColumnVector = new BytesColumnVector();
        bytesColumnVector.initBuffer(1024);
        bytesColumnVector.noNulls = !mayHaveNulls;
        batch.cols[keyIndex] = bytesColumnVector;
      }
    }
    return batch;
  }

  private void fillMixedLongStringBatch(VectorizedRowBatch batch, TypeInfo[] typeInfos, long[][] longValues,
      String[][] stringValues, boolean[][] isNull) {
    batch.size = longValues.length;
    for (int row = 0; row < batch.size; row++) {
      int longIndex = 0;
      int stringIndex = 0;
      for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
        if (typeInfos[keyIndex] == TypeInfoFactory.longTypeInfo) {
          LongColumnVector longColumnVector = (LongColumnVector) batch.cols[keyIndex];
          longColumnVector.vector[row] = longValues[row][longIndex++];
          if (isNull != null && isNull[row][keyIndex]) {
            longColumnVector.isNull[row] = true;
          }
        } else {
          BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[keyIndex];
          byte[] value = stringValues[row][stringIndex++].getBytes();
          bytesColumnVector.setVal(row, value);
          if (isNull != null && isNull[row][keyIndex]) {
            bytesColumnVector.isNull[row] = true;
          }
        }
      }
    }
  }

  private void mutateFirstStringValue(VectorizedRowBatch batch, TypeInfo[] typeInfos) {
    for (int keyIndex = 0; keyIndex < typeInfos.length; keyIndex++) {
      if (typeInfos[keyIndex] == TypeInfoFactory.stringTypeInfo) {
        ((BytesColumnVector) batch.cols[keyIndex]).vector[0][0] = 'z';
      }
    }
  }

  private void assertMixedLongStringWrapperConsistentWithGeneral(
      VectorHashKeyWrapperBase wrapper, VectorHashKeyWrapperBatch vhkwb, int longCount, int stringCount) {
    VectorHashKeyWrapperGeneral generalWrapper = new VectorHashKeyWrapperGeneral(
        new VectorHashKeyWrapperBase.HashContext(), longCount, 0, stringCount, 0, 0, 0,
        longCount + stringCount);
    for (int keyIndex = 0; keyIndex < vhkwb.keyCount; keyIndex++) {
      int typeSpecificIndex = vhkwb.columnTypeSpecificIndices[keyIndex];
      switch (vhkwb.columnVectorTypes[keyIndex]) {
      case LONG:
        if (wrapper.isNull(keyIndex)) {
          generalWrapper.assignNullLong(keyIndex, typeSpecificIndex);
        } else {
          generalWrapper.assignLong(keyIndex, typeSpecificIndex, wrapper.getLongValue(typeSpecificIndex));
        }
        break;
      case BYTES:
        if (wrapper.isNull(keyIndex)) {
          generalWrapper.assignNullString(keyIndex, typeSpecificIndex);
        } else {
          generalWrapper.assignString(typeSpecificIndex, wrapper.getBytes(typeSpecificIndex),
              wrapper.getByteStart(typeSpecificIndex), wrapper.getByteLength(typeSpecificIndex));
        }
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + vhkwb.columnVectorTypes[keyIndex]);
      }
    }
    generalWrapper.setHashKey();

    for (int keyIndex = 0; keyIndex < vhkwb.keyCount; keyIndex++) {
      assertEquals(generalWrapper.isNull(keyIndex), wrapper.isNull(keyIndex));
      int typeSpecificIndex = vhkwb.columnTypeSpecificIndices[keyIndex];
      switch (vhkwb.columnVectorTypes[keyIndex]) {
      case LONG:
        assertEquals(generalWrapper.getLongValue(typeSpecificIndex), wrapper.getLongValue(typeSpecificIndex));
        break;
      case BYTES:
        assertEquals(generalWrapper.getByteStart(typeSpecificIndex), wrapper.getByteStart(typeSpecificIndex));
        assertEquals(generalWrapper.getByteLength(typeSpecificIndex), wrapper.getByteLength(typeSpecificIndex));
        if (generalWrapper.isNull(keyIndex)) {
          assertEquals(generalWrapper.getBytes(typeSpecificIndex), wrapper.getBytes(typeSpecificIndex));
        } else {
          assertTrue(StringExpr.equal(generalWrapper.getBytes(typeSpecificIndex),
              generalWrapper.getByteStart(typeSpecificIndex), generalWrapper.getByteLength(typeSpecificIndex),
              wrapper.getBytes(typeSpecificIndex), wrapper.getByteStart(typeSpecificIndex),
              wrapper.getByteLength(typeSpecificIndex)));
        }
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + vhkwb.columnVectorTypes[keyIndex]);
      }
    }
    assertEquals(generalWrapper.stringifyKeys(vhkwb), wrapper.stringifyKeys(vhkwb));
    assertEquals(generalWrapper.toString(), wrapper.toString());
    assertEquals(generalWrapper.getVariableSize(), wrapper.getVariableSize());
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
