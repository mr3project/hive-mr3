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
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class VectorHashKeyWrapperThreeLong extends VectorHashKeyWrapperBase {

  private long longValue0;
  private long longValue1;
  private long longValue2;

  private boolean isNull0;
  private boolean isNull1;
  private boolean isNull2;

  protected VectorHashKeyWrapperThreeLong() {
    super();
    longValue0 = 0;
    longValue1 = 0;
    longValue2 = 0;
    isNull0 = false;
    isNull1 = false;
    isNull2 = false;
  }

  private static final int EMPTY_DOUBLE_ARRAY_HASH = 1;

  @Override
  public void setHashKey() {
    hashcode = calculateLongValuesHashCode() ^ EMPTY_DOUBLE_ARRAY_HASH ^ calculateNullsHashCode();
  }

  private int calculateLongValuesHashCode() {
    int result = 1;
    result = 31 * result + (int) (longValue0 ^ (longValue0 >>> 32));
    result = 31 * result + (int) (longValue1 ^ (longValue1 >>> 32));
    result = 31 * result + (int) (longValue2 ^ (longValue2 >>> 32));
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
    if (that instanceof VectorHashKeyWrapperThreeLong) {
      VectorHashKeyWrapperThreeLong keyThat = (VectorHashKeyWrapperThreeLong) that;
      return hashcode == keyThat.hashcode &&
          longValue0 == keyThat.longValue0 &&
          longValue1 == keyThat.longValue1 &&
          longValue2 == keyThat.longValue2 &&
          isNull0 == keyThat.isNull0 &&
          isNull1 == keyThat.isNull1 &&
          isNull2 == keyThat.isNull2;
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperThreeLong clone = new VectorHashKeyWrapperThreeLong();
    copyInto(clone);
    return clone;
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    VectorHashKeyWrapperThreeLong clone = (VectorHashKeyWrapperThreeLong) oldWrapper;
    copyInto(clone);
  }

  private void copyInto(VectorHashKeyWrapperThreeLong clone) {
    clone.longValue0 = longValue0;
    clone.longValue1 = longValue1;
    clone.longValue2 = longValue2;
    clone.isNull0 = isNull0;
    clone.isNull1 = isNull1;
    clone.isNull2 = isNull2;
    clone.hashcode = hashcode;
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    assignNullFlag(keyIndex, false);
    if (index == 0) {
      longValue0 = v;
    } else if (index == 1) {
      longValue1 = v;
    } else if (index == 2) {
      longValue2 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    if (index == 0) {
      longValue0 = v;
    } else if (index == 1) {
      longValue1 = v;
    } else if (index == 2) {
      longValue2 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    assignNullFlag(keyIndex, true);
    if (index == 0) {
      longValue0 = 0;
    } else if (index == 1) {
      longValue1 = 0;
    } else if (index == 2) {
      longValue2 = 0;
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

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs ");
    for (int i = 0; i < columnSetInfo.longIndices.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      int keyIndex = columnSetInfo.longIndices[i];
      if (isNull(keyIndex)) {
        sb.append("null");
      } else {
        long longValue = getLongValue(i);
        sb.append(longValue);
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
        case DATE:
          Date dt = new Date(0);
          dt.setTime(DateWritableV2.daysToMillis((int) longValue));
          sb.append(" date ");
          sb.append(dt.toString());
          break;
        default:
          break;
        }
      }
    }
    return sb.toString();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs ");
    sb.append(Arrays.toString(new long[] { longValue0, longValue1, longValue2 }));
    sb.append(", nulls ");
    sb.append(Arrays.toString(new boolean[] { isNull0, isNull1, isNull2 }));
    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    if (i == 0) {
      return longValue0;
    } else if (i == 1) {
      return longValue1;
    } else if (i == 2) {
      return longValue2;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getVariableSize() {
    return 0;
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
