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

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VLong;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.WritableUtils;

/** Decodes staged flat LazyBinary values in serialized column order. */
final class VectorLazyBinaryDeserializeBatch {
  private final PrimitiveCategory[] categories;
  private final int[] outputColumns;
  private final int[] maxLengths;
  private final VInt vInt = new VInt();
  private final VLong vLong = new VLong();

  VectorLazyBinaryDeserializeBatch(PrimitiveCategory[] categories, int[] outputColumns,
      int[] maxLengths) {
    this.categories = categories;
    this.outputColumns = outputColumns;
    this.maxLengths = maxLengths;
  }

  static boolean isSupported(PrimitiveCategory category) {
    switch (category) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
    case DATE:
    case INTERVAL_YEAR_MONTH:
      return true;
    default:
      return false;
    }
  }

  /**
   * The caller has consumed every key before lending its offset and length arrays as scratch.
   * Value offsets and lengths remain unchanged; no input references are retained.
   */
  void deserialize(VectorizedRowBatch batch, int baseRow, int rowCount, byte[] bytes,
      int[] offsets, int[] lengths, int[] positions, int[] nullBytes) throws IOException {
    System.arraycopy(offsets, 0, positions, 0, rowCount);
    for (int field = 0; field < categories.length; field++) {
      if ((field & 7) == 0) {
        // Struct null bytes are interleaved with the payload, one before every eight fields.
        for (int row = 0; row < rowCount; row++) {
          final int position = positions[row];
          checkEnd(position + 1, offsets[row] + lengths[row]);
          nullBytes[row] = bytes[position];
          positions[row] = position + 1;
        }
      }
      final int nullMask = 1 << (field & 7);
      final PrimitiveCategory category = categories[field];
      final int outputColumn = outputColumns[field];
      if (outputColumn == -1) {
        skipColumn(category, rowCount, bytes, offsets, lengths, positions, nullBytes, nullMask);
        continue;
      }
      final ColumnVector column = batch.cols[outputColumn];
      assert !column.isRepeating;
      switch (category) {
      case BOOLEAN:
        readBooleanColumn((LongColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case BYTE:
        readByteColumn((LongColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case SHORT:
        readShortColumn((LongColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case INT:
      case DATE:
      case INTERVAL_YEAR_MONTH:
        readIntColumn((LongColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case LONG:
        readLongColumn((LongColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case FLOAT:
        readFloatColumn((DoubleColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case DOUBLE:
        readDoubleColumn((DoubleColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case STRING:
      case BINARY:
        readBytesColumn((BytesColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask);
        break;
      case CHAR:
        readCharColumn((BytesColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask, maxLengths[field]);
        break;
      case VARCHAR:
        readVarcharColumn((BytesColumnVector) column, baseRow, rowCount,
            bytes, offsets, lengths, positions, nullBytes, nullMask, maxLengths[field]);
        break;
      default:
        assert false : category;
      }
    }
  }

  private static boolean isNull(ColumnVector column, int batchIndex, int nullByte, int nullMask) {
    if ((nullByte & nullMask) == 0) {
      column.isNull[batchIndex] = true;
      column.noNulls = false;
      return true;
    }
    column.isNull[batchIndex] = false;
    return false;
  }

  private static void checkEnd(int position, int end) throws EOFException {
    if (position > end) {
      throw new EOFException();
    }
  }

  private void readVInt(byte[] bytes, int position, int end) throws EOFException {
    checkEnd(position + WritableUtils.decodeVIntSize(bytes[position]), end);
    LazyBinaryUtils.readVInt(bytes, position, vInt);
  }

  private void readVLong(byte[] bytes, int position, int end) throws EOFException {
    checkEnd(position + WritableUtils.decodeVIntSize(bytes[position]), end);
    LazyBinaryUtils.readVLong(bytes, position, vLong);
  }

  private void readBooleanColumn(LongColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final long[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      checkEnd(position + 1, end);
      values[batchIndex] = bytes[position] != 0 ? 1 : 0;
      positions[row] = position + 1;
    }
  }

  private void readByteColumn(LongColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final long[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      checkEnd(position + 1, end);
      values[batchIndex] = bytes[position];
      positions[row] = position + 1;
    }
  }

  private void readShortColumn(LongColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final long[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      checkEnd(position + 2, end);
      values[batchIndex] = LazyBinaryUtils.byteArrayToShort(bytes, position);
      positions[row] = position + 2;
    }
  }

  private void readIntColumn(LongColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final long[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      readVInt(bytes, position, end);
      values[batchIndex] = vInt.value;
      positions[row] = position + vInt.length;
    }
  }

  private void readLongColumn(LongColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final long[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      readVLong(bytes, position, end);
      values[batchIndex] = vLong.value;
      positions[row] = position + vLong.length;
    }
  }

  private void readFloatColumn(DoubleColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final double[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      checkEnd(position + 4, end);
      values[batchIndex] = Float.intBitsToFloat(LazyBinaryUtils.byteArrayToInt(bytes, position));
      positions[row] = position + 4;
    }
  }

  private void readDoubleColumn(DoubleColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    final double[] values = column.vector;
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      checkEnd(position + 8, end);
      values[batchIndex] = Double.longBitsToDouble(LazyBinaryUtils.byteArrayToLong(bytes, position));
      positions[row] = position + 8;
    }
  }

  private void readBytesColumn(BytesColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask) throws IOException {
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      readVInt(bytes, position, end);
      final int start = position + vInt.length;
      final int next = start + vInt.value;
      checkEnd(next, end);
      column.setVal(batchIndex, bytes, start, vInt.value);
      positions[row] = next;
    }
  }

  private void readCharColumn(BytesColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask, int maxLength) throws IOException {
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      readVInt(bytes, position, end);
      final int start = position + vInt.length;
      final int next = start + vInt.value;
      checkEnd(next, end);
      column.setVal(batchIndex, bytes, start,
          StringExpr.rightTrimAndTruncate(bytes, start, vInt.value, maxLength));
      positions[row] = next;
    }
  }

  private void readVarcharColumn(BytesColumnVector column, int baseRow, int rowCount,
      byte[] bytes, int[] offsets, int[] lengths, int[] positions, int[] nullBytes,
      int nullMask, int maxLength) throws IOException {
    for (int row = 0; row < rowCount; row++) {
      final int batchIndex = baseRow + row;
      if (isNull(column, batchIndex, nullBytes[row], nullMask)) {
        continue;
      }
      final int position = positions[row];
      final int end = offsets[row] + lengths[row];
      readVInt(bytes, position, end);
      final int start = position + vInt.length;
      final int next = start + vInt.value;
      checkEnd(next, end);
      column.setVal(batchIndex, bytes, start, StringExpr.truncate(bytes, start, vInt.value, maxLength));
      positions[row] = next;
    }
  }

  private void skipColumn(PrimitiveCategory category, int rowCount, byte[] bytes,
      int[] offsets, int[] lengths, int[] positions, int[] nullBytes, int nullMask)
      throws IOException {
    switch (category) {
    case BOOLEAN:
    case BYTE:
      skipFixedWidthColumn(1, rowCount, offsets, lengths, positions, nullBytes, nullMask);
      break;
    case SHORT:
      skipFixedWidthColumn(2, rowCount, offsets, lengths, positions, nullBytes, nullMask);
      break;
    case FLOAT:
      skipFixedWidthColumn(4, rowCount, offsets, lengths, positions, nullBytes, nullMask);
      break;
    case DOUBLE:
      skipFixedWidthColumn(8, rowCount, offsets, lengths, positions, nullBytes, nullMask);
      break;
    case INT:
    case LONG:
    case DATE:
    case INTERVAL_YEAR_MONTH:
      for (int row = 0; row < rowCount; row++) {
        if ((nullBytes[row] & nullMask) != 0) {
          final int position = positions[row];
          final int next = position + WritableUtils.decodeVIntSize(bytes[position]);
          checkEnd(next, offsets[row] + lengths[row]);
          positions[row] = next;
        }
      }
      break;
    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
      for (int row = 0; row < rowCount; row++) {
        if ((nullBytes[row] & nullMask) != 0) {
          final int position = positions[row];
          final int end = offsets[row] + lengths[row];
          readVInt(bytes, position, end);
          final int next = position + vInt.length + vInt.value;
          checkEnd(next, end);
          positions[row] = next;
        }
      }
      break;
    default:
      assert false : category;
    }
  }

  private static void skipFixedWidthColumn(int width, int rowCount, int[] offsets, int[] lengths,
      int[] positions, int[] nullBytes, int nullMask) throws EOFException {
    for (int row = 0; row < rowCount; row++) {
      if ((nullBytes[row] & nullMask) != 0) {
        final int next = positions[row] + width;
        checkEnd(next, offsets[row] + lengths[row]);
        positions[row] = next;
      }
    }
  }
}
