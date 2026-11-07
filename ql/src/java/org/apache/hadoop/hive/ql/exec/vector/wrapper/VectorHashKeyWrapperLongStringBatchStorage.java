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

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase.HashContext;
import org.apache.hive.common.util.Murmur3;

/**
 * Column-major values shared by the fixed row views of one reusable batch.
 * Slot i and dense row r use offset i * capacity + r in each array.
 * String slices borrow input buffers until a retained key copies their bytes.
 */
final class VectorHashKeyWrapperLongStringBatchStorage {

  final int capacity;
  final int longCount;
  final int stringCount;
  final int keyCount;
  final HashContext hashCtx;

  final long[] longValues;
  final byte[][] byteValues;
  final int[] byteStarts;
  final int[] byteLengths;
  // Nulls use logical key indices; the value arrays use type-specific indices.
  final boolean[] isNull;

  final VectorHashKeyWrapperLongStringRowView[] rowViews;

  VectorHashKeyWrapperLongStringBatchStorage(HashContext hashCtx, int capacity,
      int longCount, int stringCount) {
    this.capacity = capacity;
    this.longCount = longCount;
    this.stringCount = stringCount;
    this.keyCount = longCount + stringCount;
    this.hashCtx = hashCtx;
    longValues = new long[longCount * capacity];
    byteValues = new byte[stringCount * capacity][];
    byteStarts = new int[stringCount * capacity];
    byteLengths = new int[stringCount * capacity];
    isNull = new boolean[keyCount * capacity];
    rowViews = new VectorHashKeyWrapperLongStringRowView[capacity];
    for (int row = 0; row < capacity; ++row) {
      rowViews[row] = new VectorHashKeyWrapperLongStringRowView(this, row);
    }
  }

  void clearIsNull(int size) {
    for (int keyIndex = 0; keyIndex < keyCount; ++keyIndex) {
      int offset = keyIndex * capacity;
      Arrays.fill(isNull, offset, offset + size, false);
    }
  }

  void setHashKeys(int size) {
    for (int row = 0; row < size; ++row) {
      rowViews[row].hashcode = computeHash(row);
    }
  }

  int computeHash(int row) {
    int longHash = 1;
    for (int index = 0; index < longCount; ++index) {
      long value = longValues[index * capacity + row];
      longHash = 31 * longHash + (int) (value ^ (value >>> 32));
    }
    int nullHash = 1;
    for (int keyIndex = 0; keyIndex < keyCount; ++keyIndex) {
      nullHash = 31 * nullHash + (isNull[keyIndex * capacity + row] ? 1231 : 1237);
    }
    int hash = longHash ^ nullHash;

    Murmur3.IncrementalHash32 bytesHash = null;
    int pendingOffset = -1;
    boolean hasNonNull = false;
    for (int index = 0; index < stringCount; ++index) {
      int offset = index * capacity + row;
      if (byteLengths[offset] == -1) {
        continue;
      }
      hasNonNull = true;
      if (byteLengths[offset] > 0) {
        if (pendingOffset != -1) {
          if (bytesHash == null) {
            bytesHash = HashContext.getBytesHash(hashCtx);
            bytesHash.start(hash);
          }
          bytesHash.add(
              byteValues[pendingOffset], byteStarts[pendingOffset], byteLengths[pendingOffset]);
        }
        pendingOffset = offset;
      }
    }
    if (bytesHash != null) {
      hash = bytesHash.addAndEnd(
          byteValues[pendingOffset], byteStarts[pendingOffset], byteLengths[pendingOffset]);
    } else if (pendingOffset != -1) {
      hash = Murmur3.IncrementalHash32.hash32(
          byteValues[pendingOffset], byteStarts[pendingOffset], byteLengths[pendingOffset], hash);
    } else if (hasNonNull) {
      bytesHash = HashContext.getBytesHash(hashCtx);
      bytesHash.start(hash);
      hash = bytesHash.end();
    }
    return hash;
  }

  boolean equalRows(int row, VectorHashKeyWrapperLongStringBatchStorage other, int otherRow) {
    if (rowViews[row].hashcode != other.rowViews[otherRow].hashcode ||
        longCount != other.longCount || stringCount != other.stringCount ||
        keyCount != other.keyCount) {
      return false;
    }
    for (int index = 0; index < longCount; ++index) {
      if (longValues[index * capacity + row] != other.longValues[index * other.capacity + otherRow]) {
        return false;
      }
    }
    for (int keyIndex = 0; keyIndex < keyCount; ++keyIndex) {
      if (isNull[keyIndex * capacity + row] != other.isNull[keyIndex * other.capacity + otherRow]) {
        return false;
      }
    }
    for (int index = 0; index < stringCount; ++index) {
      int offset = index * capacity + row;
      int otherOffset = index * other.capacity + otherRow;
      if (byteLengths[offset] != -1 && !StringExpr.equal(
          byteValues[offset], byteStarts[offset], byteLengths[offset],
          other.byteValues[otherOffset], other.byteStarts[otherOffset], other.byteLengths[otherOffset])) {
        return false;
      }
    }
    return true;
  }
}
