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

import java.sql.Date;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.common.util.Murmur3;

import com.google.common.base.Preconditions;

public class VectorHashKeyWrapperSingleLongTwoString extends VectorHashKeyWrapperBase {

  private long longValue0;

  private byte[] bytes0;
  private int start0;
  private int length0;

  private byte[] bytes1;
  private int start1;
  private int length1;

  private boolean isNull0;
  private boolean isNull1;
  private boolean isNull2;

  private HashContext hashCtx;

  protected VectorHashKeyWrapperSingleLongTwoString(HashContext ctx) {
    super();
    hashCtx = ctx;
    longValue0 = 0;
    bytes0 = null;
    start0 = 0;
    length0 = 0;
    bytes1 = null;
    start1 = 0;
    length1 = 0;
    isNull0 = false;
    isNull1 = false;
    isNull2 = false;
  }

  @Override
  public void setHashKey() {
    int hash = calculateLongValuesHashCode();
    hash = 31 * hash + calculateNullsHashCode();
    Murmur3.IncrementalHash32 bytesHash = null;
    if (length0 != -1) {
      bytesHash = HashContext.getBytesHash(hashCtx);
      bytesHash.start(hash);
      bytesHash.add(bytes0, start0, length0);
    }
    if (length1 != -1) {
      if (bytesHash == null) {
        bytesHash = HashContext.getBytesHash(hashCtx);
        bytesHash.start(hash);
      }
      bytesHash.add(bytes1, start1, length1);
    }
    hashcode = bytesHash == null ? hash : bytesHash.end();
  }

  private int calculateLongValuesHashCode() {
    int result = 1;
    result = 31 * result + (int) (longValue0 ^ (longValue0 >>> 32));
    return result;
  }

  private int calculateNullsHashCode() {
    int result = 1;
    result = 31 * result + (isNull0 ? 1231 : 1237);
    result = 31 * result + (isNull1 ? 1231 : 1237);
    result = 31 * result + (isNull2 ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperSingleLongTwoString) {
      VectorHashKeyWrapperSingleLongTwoString keyThat = (VectorHashKeyWrapperSingleLongTwoString) that;
      return hashcode == keyThat.hashcode &&
          longValue0 == keyThat.longValue0 &&
          isNull0 == keyThat.isNull0 &&
          isNull1 == keyThat.isNull1 &&
          isNull2 == keyThat.isNull2 &&
          (isStringNull0() || StringExpr.equal(
              bytes0, start0, length0,
              keyThat.bytes0, keyThat.start0, keyThat.length0)) &&
          (isStringNull1() || StringExpr.equal(
              bytes1, start1, length1,
              keyThat.bytes1, keyThat.start1, keyThat.length1));
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperSingleLongTwoString clone = new VectorHashKeyWrapperSingleLongTwoString(hashCtx);
    copyInto(clone);
    return clone;
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    VectorHashKeyWrapperSingleLongTwoString clone = (VectorHashKeyWrapperSingleLongTwoString) oldWrapper;
    clone.hashCtx = hashCtx;
    copyInto(clone);
  }

  private void copyInto(VectorHashKeyWrapperSingleLongTwoString clone) {
    clone.longValue0 = longValue0;
    clone.isNull0 = isNull0;
    clone.isNull1 = isNull1;
    clone.isNull2 = isNull2;
    if (isStringNull0()) {
      clone.bytes0 = null;
      clone.start0 = 0;
      clone.length0 = -1;
    } else {
      clone.bytes0 = copyBytes(bytes0, start0, length0, clone.bytes0);
      clone.start0 = 0;
      clone.length0 = length0;
    }
    if (isStringNull1()) {
      clone.bytes1 = null;
      clone.start1 = 0;
      clone.length1 = -1;
    } else {
      clone.bytes1 = copyBytes(bytes1, start1, length1, clone.bytes1);
      clone.start1 = 0;
      clone.length1 = length1;
    }
    clone.hashcode = hashcode;
    assert clone.equals(this);
  }

  private byte[] copyBytes(byte[] bytes, int start, int length, byte[] previousCopy) {
    if (previousCopy == null || previousCopy.length < length) {
      return Arrays.copyOfRange(bytes, start, start + length);
    } else {
      System.arraycopy(bytes, start, previousCopy, 0, length);
      return previousCopy;
    }
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    if (index == 0) {
      assignNullFlag(keyIndex, false);
      longValue0 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    if (index == 0) {
      longValue0 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    if (index == 0) {
      assignNullFlag(keyIndex, true);
      longValue0 = 0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignString(int index, byte[] bytes, int start, int length) {
    Preconditions.checkState(bytes != null);
    if (index == 0) {
      bytes0 = bytes;
      start0 = start;
      length0 = length;
    } else if (index == 1) {
      bytes1 = bytes;
      start1 = start;
      length1 = length;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignNullString(int keyIndex, int index) {
    assignNullFlag(keyIndex, true);
    if (index == 0) {
      bytes0 = null;
      start0 = 0;
      length0 = -1;
    } else if (index == 1) {
      bytes1 = null;
      start1 = 0;
      length1 = -1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  private void assignNullFlag(int keyIndex, boolean isNull) {
    if (keyIndex == 0) {
      isNull0 = isNull;
    } else if (keyIndex == 1) {
      isNull1 = isNull;
    } else if (keyIndex == 2) {
      isNull2 = isNull;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  private boolean isStringNull0() {
    return length0 == -1;
  }

  private boolean isStringNull1() {
    return length1 == -1;
  }

  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs ");
    appendLong(sb, columnSetInfo);
    sb.append(", byte lengths ");
    appendStringLength(sb, columnSetInfo, 0);
    sb.append(", ");
    appendStringLength(sb, columnSetInfo, 1);
    return sb.toString();
  }

  private void appendLong(StringBuilder sb, VectorColumnSetInfo columnSetInfo) {
    int keyIndex = columnSetInfo.longIndices[0];
    if (isNull(keyIndex)) {
      sb.append("null");
    } else {
      sb.append(longValue0);
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
      switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case DATE:
        Date dt = new Date(0);
        dt.setTime(DateWritableV2.daysToMillis((int) longValue0));
        sb.append(" date ");
        sb.append(dt.toString());
        break;
      default:
        break;
      }
    }
  }

  private void appendStringLength(StringBuilder sb, VectorColumnSetInfo columnSetInfo, int index) {
    int keyIndex = columnSetInfo.stringIndices[index];
    sb.append(isNull(keyIndex) ? "null" : getByteLength(index));
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs ");
    sb.append(Arrays.toString(new long[] { longValue0 }));
    sb.append(", byte lengths ");
    sb.append(Arrays.toString(new int[] { length0, length1 }));
    sb.append(", nulls ");
    sb.append(Arrays.toString(new boolean[] { isNull0, isNull1, isNull2 }));
    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    if (i == 0) {
      return longValue0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public byte[] getBytes(int i) {
    if (i == 0) {
      return bytes0;
    } else if (i == 1) {
      return bytes1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getByteStart(int i) {
    if (i == 0) {
      return start0;
    } else if (i == 1) {
      return start1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getByteLength(int i) {
    if (i == 0) {
      return length0;
    } else if (i == 1) {
      return length1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getVariableSize() {
    JavaDataModel model = JavaDataModel.get();
    return (int) (model.lengthForByteArrayOfSize(length0) + model.lengthForByteArrayOfSize(length1));
  }

  @Override
  public void clearIsNull() {
    isNull0 = false;
    isNull1 = false;
    isNull2 = false;
  }

  @Override
  public void setNull() {
    isNull0 = true;
    isNull1 = true;
    isNull2 = true;
  }

  @Override
  public boolean isNull(int keyIndex) {
    if (keyIndex == 0) {
      return isNull0;
    } else if (keyIndex == 1) {
      return isNull1;
    } else if (keyIndex == 2) {
      return isNull2;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }
}
