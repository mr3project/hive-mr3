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

import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase.HashContext;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;

/**
 * Column-major values shared by the fixed row views of one reusable batch.
 * Slot i and dense row r use offset i * capacity + r in each array.
 * String slices borrow input buffers until a retained key copies their bytes.
 * Mutable decimal, timestamp, and interval values are owned by individual slots.
 */
final class VectorHashKeyWrapperGeneralBatchStorage {

  final int capacity;
  final int longCount;
  final int doubleCount;
  final int stringCount;
  final int decimalCount;
  final int timestampCount;
  final int intervalDayTimeCount;
  final int keyCount;
  final HashContext hashCtx;

  final long[] longValues;
  final double[] doubleValues;
  final HiveDecimalWritable[] decimalValues;
  final Timestamp[] timestampValues;
  final HiveIntervalDayTime[] intervalDayTimeValues;
  final byte[][] byteValues;
  final int[] byteStarts;
  final int[] byteLengths;
  // Nulls use logical key indices; the value arrays use type-specific indices.
  final boolean[] isNull;

  final VectorHashKeyWrapperGeneralRowView[] rowViews;

  // Counts belong to this operator's wrapper role, including operations on retained keys
  // that read from this storage. Operator execution owns these counters; no atomics are needed.
  final long allocationCalls = 1;
  long clearIsNullCalls;
  long assignLongCalls;
  long assignDoubleCalls;
  long assignStringCalls;
  long assignDecimalCalls;
  long assignTimestampCalls;
  long assignIntervalDayTimeCalls;
  long computeHashCalls;
  long equalRowsCalls;
  long equalsRowCalls;
  long cloneCalls;
  long copyKeyCalls;
  long clearRowIsNullCalls;
  long setNullCalls;
  long getVariableSizeCalls;
  private boolean methodCallCountsLogged;

  VectorHashKeyWrapperGeneralBatchStorage(HashContext hashCtx, int capacity,
      int longCount, int doubleCount, int stringCount, int decimalCount,
      int timestampCount, int intervalDayTimeCount, int keyCount) {
    assert keyCount == longCount + doubleCount + stringCount + decimalCount +
        timestampCount + intervalDayTimeCount;
    this.capacity = capacity;
    this.longCount = longCount;
    this.doubleCount = doubleCount;
    this.stringCount = stringCount;
    this.decimalCount = decimalCount;
    this.timestampCount = timestampCount;
    this.intervalDayTimeCount = intervalDayTimeCount;
    this.keyCount = keyCount;
    this.hashCtx = hashCtx;
    longValues = new long[longCount * capacity];
    doubleValues = new double[doubleCount * capacity];
    decimalValues = new HiveDecimalWritable[decimalCount * capacity];
    for (int offset = 0; offset < decimalValues.length; ++offset) {
      decimalValues[offset] = new HiveDecimalWritable(HiveDecimal.ZERO);
    }
    timestampValues = new Timestamp[timestampCount * capacity];
    for (int offset = 0; offset < timestampValues.length; ++offset) {
      timestampValues[offset] = new Timestamp(0);
    }
    intervalDayTimeValues = new HiveIntervalDayTime[intervalDayTimeCount * capacity];
    for (int offset = 0; offset < intervalDayTimeValues.length; ++offset) {
      intervalDayTimeValues[offset] = new HiveIntervalDayTime();
    }
    byteValues = new byte[stringCount * capacity][];
    byteStarts = new int[stringCount * capacity];
    byteLengths = new int[stringCount * capacity];
    isNull = new boolean[keyCount * capacity];
    rowViews = new VectorHashKeyWrapperGeneralRowView[capacity];
    for (int row = 0; row < capacity; ++row) {
      rowViews[row] = new VectorHashKeyWrapperGeneralRowView(this, row);
    }
  }

  /**
   * Positional format (calls, not rows): allocation, batch null clearing, long/double/string/
   * decimal/timestamp/interval assignment - row hashing, row-view equality, retained-key equality,
   * row-view clone, copy into a retained key, row null clearing, setNull, variable-size lookup.
   */
  void logMethodCallCounts(Logger log) {
    if (!methodCallCountsLogged) {
      log.error("xxxxx {} {} {} {} {} {} {} {} - {} {} {} {} {} {} {} {}",
          allocationCalls, clearIsNullCalls, assignLongCalls, assignDoubleCalls,
          assignStringCalls, assignDecimalCalls, assignTimestampCalls, assignIntervalDayTimeCalls,
          computeHashCalls, equalRowsCalls, equalsRowCalls, cloneCalls, copyKeyCalls,
          clearRowIsNullCalls, setNullCalls, getVariableSizeCalls);
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
    int doubleHash = 1;
    for (int index = 0; index < doubleCount; ++index) {
      long bits = Double.doubleToLongBits(doubleValues[index * capacity + row]);
      doubleHash = 31 * doubleHash + (int) (bits ^ (bits >>> 32));
    }
    int nullHash = 1;
    for (int keyIndex = 0; keyIndex < keyCount; ++keyIndex) {
      nullHash = 31 * nullHash + (isNull[keyIndex * capacity + row] ? 1231 : 1237);
    }
    int hash = longHash ^ doubleHash ^ nullHash;
    for (int index = 0; index < decimalCount; ++index) {
      hash ^= decimalValues[index * capacity + row].newFasterHashCode();
    }
    for (int index = 0; index < timestampCount; ++index) {
      hash ^= timestampValues[index * capacity + row].hashCode();
    }
    for (int index = 0; index < intervalDayTimeCount; ++index) {
      hash ^= intervalDayTimeValues[index * capacity + row].hashCode();
    }

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

  boolean equalRows(int row, VectorHashKeyWrapperGeneralBatchStorage other, int otherRow) {
    ++equalRowsCalls;
    if (rowViews[row].hashcode != other.rowViews[otherRow].hashcode ||
        longCount != other.longCount || doubleCount != other.doubleCount ||
        stringCount != other.stringCount || decimalCount != other.decimalCount ||
        timestampCount != other.timestampCount || intervalDayTimeCount != other.intervalDayTimeCount ||
        keyCount != other.keyCount) {
      return false;
    }
    for (int index = 0; index < longCount; ++index) {
      if (longValues[index * capacity + row] != other.longValues[index * other.capacity + otherRow]) {
        return false;
      }
    }
    for (int index = 0; index < doubleCount; ++index) {
      if (Double.doubleToLongBits(doubleValues[index * capacity + row]) !=
          Double.doubleToLongBits(other.doubleValues[index * other.capacity + otherRow])) {
        return false;
      }
    }
    for (int index = 0; index < decimalCount; ++index) {
      if (!decimalValues[index * capacity + row].equals(
          other.decimalValues[index * other.capacity + otherRow])) {
        return false;
      }
    }
    for (int index = 0; index < timestampCount; ++index) {
      if (!timestampValues[index * capacity + row].equals(
          other.timestampValues[index * other.capacity + otherRow])) {
        return false;
      }
    }
    for (int index = 0; index < intervalDayTimeCount; ++index) {
      if (!intervalDayTimeValues[index * capacity + row].equals(
          other.intervalDayTimeValues[index * other.capacity + otherRow])) {
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
