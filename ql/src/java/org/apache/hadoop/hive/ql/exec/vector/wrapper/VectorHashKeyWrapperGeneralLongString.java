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

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hive.common.util.Murmur3;

import java.sql.Date;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.base.Preconditions;

/**
 * A hash map key wrapper for vectorized processing with only long and string key
 * columns.  This is the long/string-only equivalent of
 * {@link VectorHashKeyWrapperGeneral}; it avoids carrying fields for key types
 * that are not used by such wrappers.
 */
public class VectorHashKeyWrapperGeneralLongString extends VectorHashKeyWrapperBase {

  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private static final long[] EMPTY_LONG_ARRAY = new long[0];
  private static final byte[][] EMPTY_BYTES_ARRAY = new byte[0][];
  private long[] longValues;

  private byte[][] byteValues;
  // Each string has adjacent start and length entries at 2 * index and 2 * index + 1.
  private int[] byteStartLengths;

  private HashContext hashCtx;

  private int keyCount;

  // NOTE: The null array is indexed by keyIndex, which is not available internally.  The mapping
  //       from a long or string index to key index is kept once in the separate
  //       VectorColumnSetInfo object.
  protected boolean[] isNull;

  public VectorHashKeyWrapperGeneralLongString(HashContext ctx, int longValuesCount,
      int byteValuesCount, int keyCount) {
    super();
    hashCtx = ctx;
    this.keyCount = keyCount;
    longValues = longValuesCount > 0 ? new long[longValuesCount] : EMPTY_LONG_ARRAY;
    if (byteValuesCount > 0) {
      byteValues = new byte[byteValuesCount][];
      byteStartLengths = new int[byteValuesCount * 2];
    } else {
      byteValues = EMPTY_BYTES_ARRAY;
      byteStartLengths = EMPTY_INT_ARRAY;
    }
    isNull = new boolean[keyCount];
  }

  private VectorHashKeyWrapperGeneralLongString() {
    super();
  }

  @Override
  public void setHashKey() {
    int hash = Arrays.hashCode(longValues) ^ Arrays.hashCode(isNull);

    // This code, with branches and all, is not executed if there are no string keys.
    Murmur3.IncrementalHash32 bytesHash = null;
    int pendingIndex = -1;
    boolean hasNonNull = false;
    for (int i = 0; i < byteValues.length; ++i) {
      /*
       * Hashing the string is potentially expensive so it is better to branch.
       * Additionally not looking at values for nulls allows us not to reset the values.
       */
      int length = byteStartLengths[(i << 1) + 1];
      if (length == -1) {
        continue;
      }
      hasNonNull = true;
      if (length > 0) {
        // Keep the last nonempty string for addAndEnd; empty strings contribute no bytes.
        if (pendingIndex != -1) {
          if (bytesHash == null) {
            bytesHash = HashContext.getBytesHash(hashCtx);
            bytesHash.start(hash);
          }
          int metadataIndex = pendingIndex << 1;
          bytesHash.add(byteValues[pendingIndex],
              byteStartLengths[metadataIndex], byteStartLengths[metadataIndex + 1]);
        }
        pendingIndex = i;
      }
    }
    if (bytesHash != null) {
      int metadataIndex = pendingIndex << 1;
      hash = bytesHash.addAndEnd(byteValues[pendingIndex],
          byteStartLengths[metadataIndex], byteStartLengths[metadataIndex + 1]);
    } else if (pendingIndex != -1) {
      // A single nonempty slice can be hashed without initializing incremental state.
      int metadataIndex = pendingIndex << 1;
      hash = Murmur3.IncrementalHash32.hash32(byteValues[pendingIndex],
          byteStartLengths[metadataIndex], byteStartLengths[metadataIndex + 1], hash);
    } else if (hasNonNull) {
      // Non-null empty strings still finalize the seeded XXHash32 value.
      bytesHash = HashContext.getBytesHash(hashCtx);
      bytesHash.start(hash);
      hash = bytesHash.end();
    }
    this.hashcode = hash;
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperGeneralLongString) {
      VectorHashKeyWrapperGeneralLongString keyThat = (VectorHashKeyWrapperGeneralLongString) that;
      // not comparing hashCtx - irrelevant
      return hashcode == keyThat.hashcode &&
          Arrays.equals(longValues, keyThat.longValues) &&
          Arrays.equals(isNull, keyThat.isNull) &&
          byteValues.length == keyThat.byteValues.length &&
          (0 == byteValues.length || bytesEquals(keyThat));
    }
    return false;
  }

  private boolean bytesEquals(VectorHashKeyWrapperGeneralLongString keyThat) {
    // By the time we enter here the byteValues length and isNull must have already been compared.
    for (int i = 0; i < byteValues.length; ++i) {
      // The byte comparison is potentially expensive so it is better to branch on null.
      int metadataIndex = i << 1;
      int length = byteStartLengths[metadataIndex + 1];
      if (length != -1) {
        if (!StringExpr.equal(
            byteValues[i],
            byteStartLengths[metadataIndex],
            length,
            keyThat.byteValues[i],
            keyThat.byteStartLengths[metadataIndex],
            keyThat.byteStartLengths[metadataIndex + 1])) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperGeneralLongString clone = new VectorHashKeyWrapperGeneralLongString();
    clone.hashCtx = hashCtx;
    clone.keyCount = keyCount;
    clone.longValues = (longValues.length > 0) ? longValues.clone() : EMPTY_LONG_ARRAY;
    clone.isNull = isNull.clone();

    if (byteValues.length > 0) {
      clone.byteValues = new byte[byteValues.length][];
      clone.byteStartLengths = new int[byteStartLengths.length];
      for (int i = 0; i < byteValues.length; ++i) {
        int metadataIndex = i << 1;
        int start = byteStartLengths[metadataIndex];
        int length = byteStartLengths[metadataIndex + 1];
        // Copied bytes start at zero; the new metadata array already has zero starts.
        clone.byteStartLengths[metadataIndex + 1] = length;
        // Avoid allocation/copy of nulls, because it is potentially expensive; branch instead.
        if (length != -1) {
          clone.byteValues[i] = Arrays.copyOfRange(byteValues[i], start, start + length);
        }
      }
    } else {
      clone.byteValues = EMPTY_BYTES_ARRAY;
      clone.byteStartLengths = EMPTY_INT_ARRAY;
    }

    clone.hashcode = hashcode;
    assert clone.equals(this);

    return clone;
  }

  private long[] copyInPlaceOrAllocate(long[] from, long[] to) {
    if (from.length > 0) {
      if (to != null && to.length == from.length) {
        System.arraycopy(from, 0, to, 0, from.length);
        return to;
      } else {
        return from.clone();
      }
    } else {
      return EMPTY_LONG_ARRAY;
    }
  }

  private boolean[] copyInPlaceOrAllocate(boolean[] from, boolean[] to) {
    if (to != null && to.length == from.length) {
      System.arraycopy(from, 0, to, 0, from.length);
      return to;
    } else {
      return from.clone();
    }
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    VectorHashKeyWrapperGeneralLongString clone = (VectorHashKeyWrapperGeneralLongString) oldWrapper;
    clone.hashCtx = hashCtx;
    clone.keyCount = keyCount;
    clone.longValues = copyInPlaceOrAllocate(longValues, clone.longValues);
    clone.isNull = copyInPlaceOrAllocate(isNull, clone.isNull);

    if (byteValues.length > 0) {
      if (clone.byteStartLengths == null || clone.byteValues.length != byteValues.length) {
        clone.byteValues = new byte[byteValues.length][];
        clone.byteStartLengths = new int[byteStartLengths.length];
      }
      for (int i = 0; i < byteValues.length; ++i) {
        int metadataIndex = i << 1;
        int start = byteStartLengths[metadataIndex];
        int length = byteStartLengths[metadataIndex + 1];
        clone.byteStartLengths[metadataIndex] = 0;
        clone.byteStartLengths[metadataIndex + 1] = length;
        // Avoid allocation/copy of nulls, because it is potentially expensive; branch instead.
        if (length != -1) {
          if (clone.byteValues[i] != null && clone.byteValues[i].length >= length) {
            System.arraycopy(byteValues[i], start, clone.byteValues[i], 0, length);
          } else {
            clone.byteValues[i] = Arrays.copyOfRange(byteValues[i], start, start + length);
          }
        } else {
          clone.byteValues[i] = null;
        }
      }
    } else {
      clone.byteValues = EMPTY_BYTES_ARRAY;
      clone.byteStartLengths = EMPTY_INT_ARRAY;
    }

    clone.hashcode = hashcode;
    assert clone.equals(this);
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    isNull[keyIndex] = false;
    longValues[index] = v;
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    longValues[index] = v;
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    isNull[keyIndex] = true;
    longValues[index] = 0; // assign 0 to simplify hashcode
  }

  @Override
  public void assignString(int index, byte[] bytes, int start, int length) {
    assert (bytes != null);
    byteValues[index] = bytes;
    int metadataIndex = index << 1;
    byteStartLengths[metadataIndex] = start;
    byteStartLengths[metadataIndex + 1] = length;
  }

  @Override
  public void assignNullString(int keyIndex, int index) {
    isNull[keyIndex] = true;
    byteValues[index] = null;
    int metadataIndex = index << 1;
    byteStartLengths[metadataIndex] = 0;
    // We need some value that indicates NULL.
    byteStartLengths[metadataIndex + 1] = -1;
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo) {
    StringBuilder sb = new StringBuilder();
    boolean isFirstKey = true;

    if (longValues.length > 0) {
      isFirstKey = false;
      sb.append("longs ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.longIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.longIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(longValues[i]);
          PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
          // FUTURE: Add INTERVAL_YEAR_MONTH, etc, as desired.
          switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case DATE:
            {
              Date dt = new Date(0);
              dt.setTime(DateWritableV2.daysToMillis((int) longValues[i]));
              sb.append(" date ");
              sb.append(dt.toString());
            }
            break;
          default:
            // Add nothing more.
            break;
          }
        }
      }
    }
    if (byteValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.stringIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.stringIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(byteStartLengths[(i << 1) + 1]);
        }
      }
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    if (longValues.length > 0) {
      isFirst = false;
      sb.append("longs ");
      sb.append(Arrays.toString(longValues));
    }
    if (byteValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      sb.append('[');
      for (int i = 0; i < byteValues.length; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(byteStartLengths[(i << 1) + 1]);
      }
      sb.append(']');
    }

    if (isFirst) {
      isFirst = false;
    } else {
      sb.append(", ");
    }
    sb.append("nulls ");
    sb.append(Arrays.toString(isNull));

    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    return longValues[i];
  }

  @Override
  public byte[] getBytes(int i) {
    return byteValues[i];
  }

  @Override
  public int getByteStart(int i) {
    return byteStartLengths[i << 1];
  }

  @Override
  public int getByteLength(int i) {
    return byteStartLengths[(i << 1) + 1];
  }

  @Override
  public int getVariableSize() {
    int variableSize = 0;
    JavaDataModel model = JavaDataModel.get();
    for (int i = 1; i < byteStartLengths.length; i += 2) {
      variableSize += model.lengthForByteArrayOfSize(byteStartLengths[i]);
    }
    return variableSize;
  }

  @Override
  public void clearIsNull() {
    Arrays.fill(isNull, false);
  }

  @Override
  public void setNull() {
    Arrays.fill(isNull, true);
  }

  @Override
  public boolean isNull(int keyIndex) {
    return isNull[keyIndex];
  }
}
