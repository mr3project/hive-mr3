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
import org.slf4j.Logger;

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

  // Operator execution owns these counters, independently of GENERAL and other wrapper roles.
  final long allocationCalls = 1;
  long clearIsNullCalls;
  long assignLongBulkCopyCalls;
  long assignLongRepeatingCalls;
  long assignLongOtherCalls;
  long assignStringBulkCopyCalls;
  long assignStringRepeatingCalls;
  long assignStringOtherCalls;
  long computeHashCalls;
  long equalRowsCalls;
  long equalsRowCalls;
  long cloneCalls;
  long copyKeyCalls;
  long clearRowIsNullCalls;
  long setNullCalls;
  long getVariableSizeCalls;
  private boolean methodCallCountsLogged;

  VectorHashKeyWrapperLongStringBatchStorage(HashContext hashCtx, int capacity,
      int longCount, int stringCount, int keyCount) {
    assert keyCount == longCount + stringCount;
    this.capacity = capacity;
    this.longCount = longCount;
    this.stringCount = stringCount;
    this.keyCount = keyCount;
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

  /**
   * Positional format (calls, not rows): allocation, batch null clearing, long bulk copy/repeating/
   * other assignment, string bulk copy/repeating/other assignment - row hashing, row-view equality,
   * retained-key equality, row-view clone, copy into a retained key, row null clearing, setNull,
   * variable-size lookup. Repeating string assignment includes assignNullStrings.
   */
  void logMethodCallCounts(Logger log) {
    if (!methodCallCountsLogged) {
      log.error("yyyyy {} {} {} {} {} {} {} {} - {} {} {} {} {} {} {} {}",
          allocationCalls, clearIsNullCalls, assignLongBulkCopyCalls, assignLongRepeatingCalls,
          assignLongOtherCalls, assignStringBulkCopyCalls, assignStringRepeatingCalls,
          assignStringOtherCalls, computeHashCalls, equalRowsCalls, equalsRowCalls, cloneCalls,
          copyKeyCalls, clearRowIsNullCalls, setNullCalls, getVariableSizeCalls);
      methodCallCountsLogged = true;
    }
  }

  void clearIsNull(int size) {
    ++clearIsNullCalls;
    assert size >= 0 && size <= capacity;
    for (int keyIndex = 0; keyIndex < keyCount; ++keyIndex) {
      int offset = keyIndex * capacity;
      Arrays.fill(isNull, offset, offset + size, false);
    }
  }

  void setHashKeys(int size) {
    assert size >= 0 && size <= capacity;
    for (int row = 0; row < size; ++row) {
      rowViews[row].hashcode = computeHash(row);
    }
  }

  int computeHash(int row) {
    ++computeHashCalls;
    assert row >= 0 && row < capacity;
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
    ++equalRowsCalls;
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
