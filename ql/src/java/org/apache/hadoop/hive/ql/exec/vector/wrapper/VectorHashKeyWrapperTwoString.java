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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hive.common.util.Murmur3;

import com.google.common.base.Preconditions;

public class VectorHashKeyWrapperTwoString extends VectorHashKeyWrapperTwoBase {

  private byte[] bytes0;
  private int start0;
  private int length0;

  private byte[] bytes1;
  private int start1;
  private int length1;

  private static final int nonNullHashcode = Arrays.hashCode(new boolean[] { false, false });
  private static final int null0Hashcode = Arrays.hashCode(new boolean[] { true, false });
  private static final int null1Hashcode = Arrays.hashCode(new boolean[] { false, true });
  private static final int twoNullHashcode = Arrays.hashCode(new boolean[] { true, true });

  private HashContext hashCtx;

  protected VectorHashKeyWrapperTwoString(HashContext ctx) {
    super();
    hashCtx = ctx;
    bytes0 = null;
    start0 = 0;
    length0 = 0;
    bytes1 = null;
    start1 = 0;
    length1 = 0;
  }

  @Override
  public void setHashKey() {
    int hash;
    if (isNull0 || isNull1) {
      hash = isNull0 && isNull1 ? twoNullHashcode : isNull0 ? null0Hashcode : null1Hashcode;
    } else {
      hash = nonNullHashcode;
    }

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

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperTwoString) {
      VectorHashKeyWrapperTwoString keyThat = (VectorHashKeyWrapperTwoString) that;
      return isNull0 == keyThat.isNull0 &&
          (isNull0 || StringExpr.equal(
              bytes0, start0, length0,
              keyThat.bytes0, keyThat.start0, keyThat.length0)) &&
          isNull1 == keyThat.isNull1 &&
          (isNull1 || StringExpr.equal(
              bytes1, start1, length1,
              keyThat.bytes1, keyThat.start1, keyThat.length1));
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperTwoString clone = new VectorHashKeyWrapperTwoString(hashCtx);
    copyInto(clone);
    return clone;
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    VectorHashKeyWrapperTwoString clone = (VectorHashKeyWrapperTwoString) oldWrapper;
    clone.hashCtx = hashCtx;
    copyInto(clone);
  }

  private void copyInto(VectorHashKeyWrapperTwoString clone) {
    clone.isNull0 = isNull0;
    clone.isNull1 = isNull1;
    if (isNull0) {
      clone.bytes0 = null;
      clone.start0 = 0;
      clone.length0 = -1;
    } else {
      clone.bytes0 = copyBytes(bytes0, start0, length0, clone.bytes0);
      clone.start0 = 0;
      clone.length0 = length0;
    }
    if (isNull1) {
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
  public void assignString(int index, byte[] bytes, int start, int length) {
    assert (bytes != null);
    if (index == 0) {
      isNull0 = false;
      bytes0 = bytes;
      start0 = start;
      length0 = length;
    } else if (index == 1) {
      isNull1 = false;
      bytes1 = bytes;
      start1 = start;
      length1 = length;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignNullString(int keyIndex, int index) {
    if (keyIndex == 0 && index == 0) {
      isNull0 = true;
      bytes0 = null;
      start0 = 0;
      length0 = -1;
    } else if (keyIndex == 1 && index == 1) {
      isNull1 = true;
      bytes1 = null;
      start1 = 0;
      length1 = -1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("byte lengths ");
    if (!isNull0) {
      sb.append(length0);
    } else {
      sb.append("null");
    }
    sb.append(", ");
    if (!isNull1) {
      sb.append(length1);
    } else {
      sb.append("null");
    }
    return sb.toString();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("byte lengths [");
    sb.append(length0);
    sb.append(", ");
    sb.append(length1);
    sb.append("], nulls [");
    sb.append(isNull0);
    sb.append(", ");
    sb.append(isNull1);
    sb.append("]");
    return sb.toString();
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
}
