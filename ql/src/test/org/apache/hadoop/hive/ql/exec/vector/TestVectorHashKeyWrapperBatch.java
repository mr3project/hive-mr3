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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleString;
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

    VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) vhkwArray[0].copyKey();
    assertTrue(copy instanceof VectorHashKeyWrapperSingleString);
    assertEquals(vhkwArray[0], copy);
    assertEquals(vhkwArray[0].hashCode(), copy.hashCode());

    bytesColumnVector.vector[0][0] = 'z';
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        alpha, 0, alpha.length));
    assertFalse(StringExpr.equal(vhkwArray[0].getBytes(0), vhkwArray[0].getByteStart(0),
        vhkwArray[0].getByteLength(0), copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0)));

    vhkwArray[1].copyKey(copy);
    assertEquals(vhkwArray[1], copy);
    assertTrue(StringExpr.equal(copy.getBytes(0), copy.getByteStart(0), copy.getByteLength(0),
        beta, 0, beta.length));
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
  }

}
