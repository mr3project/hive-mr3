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

import static org.apache.tez.util.FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.tez.util.FastByteComparisons.theUnsafe;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;

/** Scalar payload operations shared by direct and accumulated compact shuffle frames. */
final class VectorShuffleBatchEncoding {

  static final int IS_REPEATING = 1;
  static final int HAS_NULLS = 2;
  static final int IS_DECIMAL_64 = 4;

  private VectorShuffleBatchEncoding() {
  }

  /** Returns -1 for variable-width scalars and complex schema nodes. */
  static int fixedWidth(ColumnVector column) {
    if (column instanceof TimestampColumnVector || column instanceof IntervalDayTimeColumnVector) {
      return Long.BYTES + Integer.BYTES;
    }
    if (column instanceof LongColumnVector || column instanceof DoubleColumnVector) {
      return Long.BYTES;
    }
    return column instanceof VoidColumnVector ? 0 : -1;
  }

  /**
   * Writes one non-null scalar in the existing representation, returning its encoded length.
   * The caller resolves source repetition before passing the physical index. Variable payload
   * overflows may be retried into grown storage; all unsafe writes are preceded by bounds checks.
   */
  static int writeScalar(ColumnVector column, int index, byte[] buffer, int offset) {
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytes = (BytesColumnVector) column;
      int length = bytes.length[index];
      if (length < 0) {
        throw new IllegalArgumentException("Negative byte length: " + length);
      }
      ensureCapacity(buffer, offset, (long) Integer.BYTES + length);
      System.arraycopy(bytes.vector[index], bytes.start[index], buffer, offset + Integer.BYTES,
          length);
      putInt(buffer, offset, length);
      return Integer.BYTES + length;
    }
    if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      ensureCapacity(buffer, offset, Long.BYTES + Integer.BYTES);
      putLong(buffer, offset, timestamp.time[index]);
      putInt(buffer, offset + Long.BYTES, timestamp.nanos[index]);
      return Long.BYTES + Integer.BYTES;
    }
    if (column instanceof IntervalDayTimeColumnVector) {
      ensureCapacity(buffer, offset, Long.BYTES + Integer.BYTES);
      HiveIntervalDayTime interval =
          ((IntervalDayTimeColumnVector) column).asScratchIntervalDayTime(index);
      putLong(buffer, offset, interval.getTotalSeconds());
      putInt(buffer, offset + Long.BYTES, interval.getNanos());
      return Long.BYTES + Integer.BYTES;
    }
    if (column instanceof DecimalColumnVector) {
      ensureCapacity(buffer, offset, 0);
      // writeDirect uses checked array accesses and preserves the decimal's existing encoding.
      return ((DecimalColumnVector) column).vector[index].writeDirect(buffer, offset);
    }
    if (column instanceof LongColumnVector) {
      ensureCapacity(buffer, offset, Long.BYTES);
      putLong(buffer, offset, ((LongColumnVector) column).vector[index]);
      return Long.BYTES;
    }
    if (column instanceof DoubleColumnVector) {
      ensureCapacity(buffer, offset, Double.BYTES);
      // Canonicalize NaN payloads while retaining the distinction between signed zeros.
      putLong(buffer, offset, Double.doubleToLongBits(((DoubleColumnVector) column).vector[index]));
      return Double.BYTES;
    }
    if (column instanceof VoidColumnVector) {
      return 0;
    }
    throw new IllegalArgumentException(
        "Unsupported vector shuffle scalar type " + column.getClass().getName());
  }

  // Callers reserve complete payloads before these native-endian primitive writes.
  static void putInt(byte[] buffer, int offset, int value) {
    theUnsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET + offset, value);
  }

  static void putLong(byte[] buffer, int offset, long value) {
    theUnsafe.putLong(buffer, BYTE_ARRAY_BASE_OFFSET + offset, value);
  }

  private static void ensureCapacity(byte[] buffer, int offset, long length) {
    if (offset < 0 || length < 0 || length > buffer.length - (long) offset) {
      throw new ArrayIndexOutOfBoundsException(
          "position=" + offset + ", length=" + length + ", limit=" + buffer.length);
    }
  }
}
