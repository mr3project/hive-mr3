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

/**
 * Native-endian fixed-width payload movement for the existing compact shuffle frame format.
 * The format writes longs with Unsafe.putLong and doubles with Double.doubleToLongBits.
 * Raw double copying is safe only when reading those already-serialized bits,
 * not when writing arbitrary source doubles (which may contain non-canonical NaN payloads).
 */
final class VectorShuffleBatchFixedWidth {

  private static final int BULK_COPY_MIN_ROWS = 2;
  private static final long LONG_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(long[].class);
  private static final long DOUBLE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(double[].class);

  private VectorShuffleBatchFixedWidth() {
  }

  static boolean canRead(ColumnVector column, boolean sourceIsDecimal64) {
    if (sourceIsDecimal64) {
      return column instanceof Decimal64ColumnVector;
    }
    return column instanceof DoubleColumnVector
        || (column instanceof LongColumnVector && !(column instanceof Decimal64ColumnVector));
  }

  /** The caller checks source/destination bounds and handles null-vector state. */
  static void read(byte[] source, int sourceOffset, ColumnVector destination,
      int destinationOffset, int count) {
    if (count == 0) {
      return;
    }

    if (destination instanceof DoubleColumnVector) {
      double[] values = ((DoubleColumnVector) destination).vector;
      if (count >= BULK_COPY_MIN_ROWS) {
        theUnsafe.copyMemory(source, BYTE_ARRAY_BASE_OFFSET + sourceOffset,
            values, DOUBLE_ARRAY_BASE_OFFSET + (long) destinationOffset * Double.BYTES,
            (long) count * Double.BYTES);
      } else {
        values[destinationOffset] = Double.longBitsToDouble(
            theUnsafe.getLong(source, BYTE_ARRAY_BASE_OFFSET + sourceOffset));
      }
    } else {
      long[] values = ((LongColumnVector) destination).vector;
      if (count >= BULK_COPY_MIN_ROWS) {
        theUnsafe.copyMemory(source, BYTE_ARRAY_BASE_OFFSET + sourceOffset,
            values, LONG_ARRAY_BASE_OFFSET + (long) destinationOffset * Long.BYTES,
            (long) count * Long.BYTES);
      } else {
        values[destinationOffset] =
            theUnsafe.getLong(source, BYTE_ARRAY_BASE_OFFSET + sourceOffset);
      }
    }
  }

  /** The caller checks capacity; only the primitive long representation is copied raw. */
  static void writeLongs(long[] source, int sourceOffset, byte[] destination,
      int destinationOffset, int count) {
    if (count == 0) {
      return;
    }

    if (count >= BULK_COPY_MIN_ROWS) {
      theUnsafe.copyMemory(source, LONG_ARRAY_BASE_OFFSET + (long) sourceOffset * Long.BYTES,
          destination, BYTE_ARRAY_BASE_OFFSET + destinationOffset, (long) count * Long.BYTES);
    } else {
      theUnsafe.putLong(
          destination, BYTE_ARRAY_BASE_OFFSET + destinationOffset,
          source[sourceOffset]);
    }
  }
}
