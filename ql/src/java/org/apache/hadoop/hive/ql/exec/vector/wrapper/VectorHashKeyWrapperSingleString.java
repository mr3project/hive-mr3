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

public class VectorHashKeyWrapperSingleString extends VectorHashKeyWrapperSingleBase {

  private byte[] bytes0;
  private int start0;
  private int length0;

  private HashContext hashCtx;

  protected VectorHashKeyWrapperSingleString(HashContext ctx) {
    super();
    hashCtx = ctx;
    bytes0 = null;
    start0 = 0;
    length0 = 0;
  }

  @Override
  public void setHashKey() {
    if (isNull0) {
      hashcode = nullHashcode;
    } else {
      Murmur3.IncrementalHash32 bytesHash = HashContext.getBytesHash(hashCtx);
      bytesHash.start(0);
      bytesHash.add(bytes0, start0, length0);
      hashcode = bytesHash.end();
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperSingleString) {
      VectorHashKeyWrapperSingleString keyThat = (VectorHashKeyWrapperSingleString) that;
      return isNull0 == keyThat.isNull0 &&
          (isNull0 || StringExpr.equal(
              bytes0, start0, length0,
              keyThat.bytes0, keyThat.start0, keyThat.length0));
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperSingleString clone = new VectorHashKeyWrapperSingleString(hashCtx);
    copyInto(clone);
    return clone;
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    VectorHashKeyWrapperSingleString clone = (VectorHashKeyWrapperSingleString) oldWrapper;
    clone.hashCtx = hashCtx;
    copyInto(clone);
  }

  private void copyInto(VectorHashKeyWrapperSingleString clone) {
    clone.isNull0 = isNull0;
    if (isNull0) {
      clone.bytes0 = null;
      clone.start0 = 0;
      clone.length0 = -1;
    } else {
      if (clone.bytes0 == null || clone.bytes0.length < length0) {
        clone.bytes0 = Arrays.copyOfRange(bytes0, start0, start0 + length0);
      } else {
        System.arraycopy(bytes0, start0, clone.bytes0, 0, length0);
      }
      clone.start0 = 0;
      clone.length0 = length0;
    }
    clone.hashcode = hashcode;
    assert clone.equals(this);
  }

  @Override
  public void assignString(int index, byte[] bytes, int start, int length) {
    if (index == 0) {
      Preconditions.checkState(bytes != null);
      isNull0 = false;
      bytes0 = bytes;
      start0 = start;
      length0 = length;
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
    sb.append("bytes lengths [");
    if (!isNull0) {
      sb.append(length0);
    } else {
      sb.append("null");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("bytes lengths [");
    sb.append(length0);
    sb.append("], nulls [");
    sb.append(isNull0);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public byte[] getBytes(int i) {
    if (i == 0) {
      return bytes0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getByteStart(int i) {
    if (i == 0) {
      return start0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getByteLength(int i) {
    if (i == 0) {
      return length0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getVariableSize() {
    return isNull0 ? 0 : JavaDataModel.get().lengthForByteArrayOfSize(length0);
  }
}
