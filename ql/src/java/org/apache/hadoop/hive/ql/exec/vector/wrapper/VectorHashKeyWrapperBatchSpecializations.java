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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/** Bulk loops use consolidated storage or a concrete wrapper type. */
final class VectorHashKeyWrapperBatchSpecializations {
  private VectorHashKeyWrapperBatchSpecializations() {
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.longValues, offset, offset + size, 0);
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size) {
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.longValues, offset, offset + size, 0);
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = r;
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = r;
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, LongColumnVector columnVector) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    Arrays.fill(storage.longValues, offset, offset + size, columnVector.vector[0]);
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, LongColumnVector columnVector) {
    final int offset = index * storage.capacity;
    Arrays.fill(storage.longValues, offset, offset + size, columnVector.vector[0]);
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      values[offset + r] = sourceValues[sourceRow];
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    final int offset = index * storage.capacity;
    final long[] values = storage.longValues;
    final long[] sourceValues = columnVector.vector;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      values[offset + r] = sourceValues[sourceRow];
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, LongColumnVector columnVector) {
    ++storage.assignLongCalls;
    final int offset = index * storage.capacity;
    System.arraycopy(columnVector.vector, 0, storage.longValues, offset, size);
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, LongColumnVector columnVector) {
    final int offset = index * storage.capacity;
    System.arraycopy(columnVector.vector, 0, storage.longValues, offset, size);
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignStringNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, BytesColumnVector columnVector, int[] selected) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      final int destination = offset + r;
      if (sourceNulls[sourceRow]) {
        values[destination] = null;
        // starts[destination] = 0;    // optional: null strings are skipped before their offsets are read
        lengths[destination] = -1;
        nulls[nullOffset + r] = true;
      } else {
        values[destination] = sourceValues[sourceRow];
        starts[destination] = sourceStarts[sourceRow];
        lengths[destination] = sourceLengths[sourceRow];
      }
    }
  }

  static void assignStringNullsNoRepeatingSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size, BytesColumnVector columnVector, int[] selected) {
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      final int destination = offset + r;
      if (sourceNulls[sourceRow]) {
        values[destination] = null;
        // starts[destination] = 0;    // optional: null strings are skipped before their offsets are read
        lengths[destination] = -1;
        nulls[nullOffset + r] = true;
      } else {
        values[destination] = sourceValues[sourceRow];
        starts[destination] = sourceStarts[sourceRow];
        lengths[destination] = sourceLengths[sourceRow];
      }
    }
  }

  static void assignStringNullsNoRepeatingSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (columnVector.isNull[row]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[row],
            columnVector.start[row],
            columnVector.length[row]);
      }
    }
  }

  static void assignStringNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, BytesColumnVector columnVector) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = r;
      final int destination = offset + r;
      if (sourceNulls[sourceRow]) {
        values[destination] = null;
        // starts[destination] = 0;    // optional: null strings are skipped before their offsets are read
        lengths[destination] = -1;
        nulls[nullOffset + r] = true;
      } else {
        values[destination] = sourceValues[sourceRow];
        starts[destination] = sourceStarts[sourceRow];
        lengths[destination] = sourceLengths[sourceRow];
      }
    }
  }

  static void assignStringNullsNoRepeatingNoSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size, BytesColumnVector columnVector) {
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = r;
      final int destination = offset + r;
      if (sourceNulls[sourceRow]) {
        values[destination] = null;
        // starts[destination] = 0;    // optional: null strings are skipped before their offsets are read
        lengths[destination] = -1;
        nulls[nullOffset + r] = true;
      } else {
        values[destination] = sourceValues[sourceRow];
        starts[destination] = sourceStarts[sourceRow];
        lengths[destination] = sourceLengths[sourceRow];
      }
    }
  }

  static void assignStringNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      if (columnVector.isNull[i]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[i],
            columnVector.start[i],
            columnVector.length[i]);
      }
    }
  }

  static void assignStringNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, BytesColumnVector columnVector) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    Arrays.fill(storage.byteValues, offset, offset + size, columnVector.vector[0]);
    Arrays.fill(storage.byteStarts, offset, offset + size, columnVector.start[0]);
    Arrays.fill(storage.byteLengths, offset, offset + size, columnVector.length[0]);
  }

  static void assignStringNoNullsRepeating(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, BytesColumnVector columnVector) {
    final int offset = index * storage.capacity;
    Arrays.fill(storage.byteValues, offset, offset + size, columnVector.vector[0]);
    Arrays.fill(storage.byteStarts, offset, offset + size, columnVector.start[0]);
    Arrays.fill(storage.byteLengths, offset, offset + size, columnVector.length[0]);
  }

  static void assignStringNoNullsRepeating(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[0],
          columnVector.start[0],
          columnVector.length[0]);
    }
  }

  static void assignStringNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, BytesColumnVector columnVector, int[] selected) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      final int destination = offset + r;
      values[destination] = sourceValues[sourceRow];
      starts[destination] = sourceStarts[sourceRow];
      lengths[destination] = sourceLengths[sourceRow];
    }
  }

  static void assignStringNoNullsNoRepeatingSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, BytesColumnVector columnVector, int[] selected) {
    final int offset = index * storage.capacity;
    final byte[][] values = storage.byteValues;
    final int[] starts = storage.byteStarts;
    final int[] lengths = storage.byteLengths;
    final byte[][] sourceValues = columnVector.vector;
    final int[] sourceStarts = columnVector.start;
    final int[] sourceLengths = columnVector.length;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      final int destination = offset + r;
      values[destination] = sourceValues[sourceRow];
      starts[destination] = sourceStarts[sourceRow];
      lengths[destination] = sourceLengths[sourceRow];
    }
  }

  static void assignStringNoNullsNoRepeatingSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      wrappers[i].assignString(
          index,
          columnVector.vector[row],
          columnVector.start[row],
          columnVector.length[row]);
    }
  }

  static void assignStringNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, BytesColumnVector columnVector) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    System.arraycopy(columnVector.vector, 0, storage.byteValues, offset, size);
    System.arraycopy(columnVector.start, 0, storage.byteStarts, offset, size);
    System.arraycopy(columnVector.length, 0, storage.byteLengths, offset, size);
  }

  static void assignStringNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperLongStringBatchStorage storage,
      int index, int size, BytesColumnVector columnVector) {
    final int offset = index * storage.capacity;
    System.arraycopy(columnVector.vector, 0, storage.byteValues, offset, size);
    System.arraycopy(columnVector.start, 0, storage.byteStarts, offset, size);
    System.arraycopy(columnVector.length, 0, storage.byteLengths, offset, size);
  }

  static void assignStringNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[i],
          columnVector.start[i],
          columnVector.length[i]);
    }
  }

  static void clearIsNull(VectorHashKeyWrapperGeneralBatchStorage storage, int size) {
    storage.clearIsNull(size);
  }

  static void clearIsNull(VectorHashKeyWrapperLongStringBatchStorage storage, int size) {
    storage.clearIsNull(size);
  }

  static void clearIsNull(VectorHashKeyWrapperSingleLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperTwoLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperThreeLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperThreeString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperGeneralBatchStorage storage, int size) {
    storage.setHashKeys(size);
  }

  static void setHashKeys(VectorHashKeyWrapperLongStringBatchStorage storage, int size) {
    storage.setHashKeys(size);
  }

  static void setHashKeys(VectorHashKeyWrapperSingleLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperTwoLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperThreeLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperThreeString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void assignNullStrings(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignStringCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.byteValues, offset, offset + size, null);
    // Arrays.fill(storage.byteStarts, offset, offset + size, 0);    // optional: null strings are skipped before their offsets are read
    Arrays.fill(storage.byteLengths, offset, offset + size, -1);
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
  }

  static void assignNullStrings(VectorHashKeyWrapperLongStringBatchStorage storage,
      int keyIndex, int index, int size) {
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.byteValues, offset, offset + size, null);
    // Arrays.fill(storage.byteStarts, offset, offset + size, 0);    // optional: null strings are skipped before their offsets are read
    Arrays.fill(storage.byteLengths, offset, offset + size, -1);
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
  }

  static void assignNullStrings(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignNullString(keyIndex, index);
    }
  }

  static void assignDoubleNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, DoubleColumnVector columnVector, int[] selected) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    final double[] values = storage.doubleValues;
    final double[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignDoubleNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.doubleValues, offset, offset + size, 0);
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
  }

  static void assignDoubleNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, DoubleColumnVector columnVector) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    final double[] values = storage.doubleValues;
    final double[] sourceValues = columnVector.vector;
    final boolean[] nulls = storage.isNull;
    final boolean[] sourceNulls = columnVector.isNull;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = r;
      if (sourceNulls[sourceRow]) {
        values[offset + r] = 0;
        nulls[nullOffset + r] = true;
      } else {
        values[offset + r] = sourceValues[sourceRow];
      }
    }
  }

  static void assignDoubleNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DoubleColumnVector columnVector) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    Arrays.fill(storage.doubleValues, offset, offset + size, columnVector.vector[0]);
  }

  static void assignDoubleNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DoubleColumnVector columnVector, int[] selected) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    final double[] values = storage.doubleValues;
    final double[] sourceValues = columnVector.vector;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      values[offset + r] = sourceValues[sourceRow];
    }
  }

  static void assignDoubleNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DoubleColumnVector columnVector) {
    ++storage.assignDoubleCalls;
    final int offset = index * storage.capacity;
    System.arraycopy(columnVector.vector, 0, storage.doubleValues, offset, size);
  }

  static void assignDecimalNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, DecimalColumnVector columnVector, int[] selected) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (columnVector.isNull[sourceRow]) {
        storage.isNull[nullOffset + r] = true;
        storage.decimalValues[offset + r].set(HiveDecimal.ZERO);
      } else {
        storage.decimalValues[offset + r].set(columnVector.vector[sourceRow]);
      }
    }
  }

  static void assignDecimalNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, DecimalColumnVector columnVector) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      if (columnVector.isNull[r]) {
        storage.isNull[nullOffset + r] = true;
        storage.decimalValues[offset + r].set(HiveDecimal.ZERO);
      } else {
        storage.decimalValues[offset + r].set(columnVector.vector[r]);
      }
    }
  }

  static void assignDecimalNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DecimalColumnVector columnVector) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      storage.decimalValues[offset + r].set(columnVector.vector[0]);
    }
  }

  static void assignDecimalNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DecimalColumnVector columnVector, int[] selected) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      storage.decimalValues[offset + r].set(columnVector.vector[sourceRow]);
    }
  }

  static void assignDecimalNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, DecimalColumnVector columnVector) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      storage.decimalValues[offset + r].set(columnVector.vector[r]);
    }
  }

  static void assignDecimalNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignDecimalCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
    for (int r = 0; r < size; ++r) {
      storage.decimalValues[offset + r].set(HiveDecimal.ZERO);
    }
  }

  static void assignTimestampNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, TimestampColumnVector columnVector, int[] selected) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (columnVector.isNull[sourceRow]) {
        storage.isNull[nullOffset + r] = true;
        storage.timestampValues[offset + r].setTime(0);
      } else {
        columnVector.timestampUpdate(storage.timestampValues[offset + r], sourceRow);
      }
    }
  }

  static void assignTimestampNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, TimestampColumnVector columnVector) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      if (columnVector.isNull[r]) {
        storage.isNull[nullOffset + r] = true;
        storage.timestampValues[offset + r].setTime(0);
      } else {
        columnVector.timestampUpdate(storage.timestampValues[offset + r], r);
      }
    }
  }

  static void assignTimestampNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, TimestampColumnVector columnVector) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      columnVector.timestampUpdate(storage.timestampValues[offset + r], 0);
    }
  }

  static void assignTimestampNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, TimestampColumnVector columnVector, int[] selected) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      columnVector.timestampUpdate(storage.timestampValues[offset + r], sourceRow);
    }
  }

  static void assignTimestampNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, TimestampColumnVector columnVector) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      columnVector.timestampUpdate(storage.timestampValues[offset + r], r);
    }
  }

  static void assignTimestampNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignTimestampCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
    for (int r = 0; r < size; ++r) {
      storage.timestampValues[offset + r].setTime(0);
    }
  }

  static void assignIntervalDayTimeNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, IntervalDayTimeColumnVector columnVector, int[] selected) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      if (columnVector.isNull[sourceRow]) {
        storage.isNull[nullOffset + r] = true;
        storage.intervalDayTimeValues[offset + r].set(0, 0);
      } else {
        storage.intervalDayTimeValues[offset + r].set(columnVector.asScratchIntervalDayTime(sourceRow));
      }
    }
  }

  static void assignIntervalDayTimeNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size, IntervalDayTimeColumnVector columnVector) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    for (int r = 0; r < size; ++r) {
      if (columnVector.isNull[r]) {
        storage.isNull[nullOffset + r] = true;
        storage.intervalDayTimeValues[offset + r].set(0, 0);
      } else {
        storage.intervalDayTimeValues[offset + r].set(columnVector.asScratchIntervalDayTime(r));
      }
    }
  }

  static void assignIntervalDayTimeNoNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, IntervalDayTimeColumnVector columnVector) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      storage.intervalDayTimeValues[offset + r].set(columnVector.asScratchIntervalDayTime(0));
    }
  }

  static void assignIntervalDayTimeNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, IntervalDayTimeColumnVector columnVector, int[] selected) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      final int sourceRow = selected[r];
      storage.intervalDayTimeValues[offset + r].set(columnVector.asScratchIntervalDayTime(sourceRow));
    }
  }

  static void assignIntervalDayTimeNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralBatchStorage storage,
      int index, int size, IntervalDayTimeColumnVector columnVector) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    for (int r = 0; r < size; ++r) {
      storage.intervalDayTimeValues[offset + r].set(columnVector.asScratchIntervalDayTime(r));
    }
  }

  static void assignIntervalDayTimeNullsRepeating(VectorHashKeyWrapperGeneralBatchStorage storage,
      int keyIndex, int index, int size) {
    ++storage.assignIntervalDayTimeCalls;
    final int offset = index * storage.capacity;
    final int nullOffset = keyIndex * storage.capacity;
    Arrays.fill(storage.isNull, nullOffset, nullOffset + size, true);
    for (int r = 0; r < size; ++r) {
      storage.intervalDayTimeValues[offset + r].set(0, 0);
    }
  }
}
