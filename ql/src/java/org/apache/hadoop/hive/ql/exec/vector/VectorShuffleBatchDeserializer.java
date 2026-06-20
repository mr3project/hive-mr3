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

import java.io.IOException;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;

/** Deserializes a compact vector shuffle payload into an already schema-initialized batch. */
public final class VectorShuffleBatchDeserializer {
  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;
  private static final int IS_DECIMAL_64 = 4;

  private byte[] buffer;
  private int position;
  private int limit;

  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    if (serialized == null || destination == null) {
      throw new IllegalArgumentException("Serialized batch and destination are required");
    }

    buffer = serialized.getBytes();
    position = 0;
    limit = serialized.getLength();
    final int rowCount = readInt();
    final int columnCount = readInt();
    if (rowCount < 0 || columnCount < 0 || columnCount > destination.cols.length) {
      throw new IOException("Invalid vector shuffle batch dimensions: " + rowCount + " rows, "
          + columnCount + " columns");
    }

    destination.reset();
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      if (destination.cols[columnIndex] != null) {
        destination.cols[columnIndex].ensureSize(rowCount, false);
      }
    }
    destination.size = rowCount;
    destination.selectedInUse = false;
    destination.projectionSize = columnCount;
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      destination.projectedColumns[columnIndex] = columnIndex;
      ColumnVector column = destination.cols[columnIndex];
      if (column == null) {
        throw new IOException("Destination column " + columnIndex + " is not initialized");
      }
      readColumn(column, rowCount);
    }
    if (position != limit) {
      throw new IOException("Vector shuffle batch has " + (limit - position) + " trailing bytes");
    }
  }

  private void readColumn(ColumnVector column, int rowCount) throws IOException {
    readColumn(column, null, rowCount);
  }

  private void readColumn(ColumnVector column, int[] destinationIndices, int rowCount)
      throws IOException {
    final int flags = readUnsignedByte();
    final boolean repeating = (flags & IS_REPEATING) != 0;
    final boolean hasNulls = (flags & HAS_NULLS) != 0;
    final boolean sourceIsDecimal64 = (flags & IS_DECIMAL_64) != 0;
    if (sourceIsDecimal64
        && !(column instanceof Decimal64ColumnVector || column instanceof DecimalColumnVector)) {
      throw new IOException("Decimal64 vector payload cannot be decoded into "
          + column.getClass().getName());
    }
    final int valueCount = repeating ? Math.min(rowCount, 1) : rowCount;
    column.ensureSize(requiredSize(destinationIndices, valueCount, repeating), false);

    byte[] nullBitmap = null;
    if (hasNulls) {
      nullBitmap = new byte[(valueCount + 7) / 8];
      readFully(nullBitmap);
    }

    column.isRepeating = repeating;
    column.noNulls = !hasNulls;
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      column.isNull[destinationIndex] = hasNulls && isNull(nullBitmap, logical);
    }
    readTypeMetadata(column);

    if (column instanceof StructColumnVector) {
      readStructChildren((StructColumnVector) column, destinationIndices, valueCount, repeating);
    } else if (column instanceof UnionColumnVector) {
      readUnionChildren((UnionColumnVector) column, destinationIndices, valueCount, repeating);
    } else if (column instanceof ListColumnVector) {
      ListColumnVector list = (ListColumnVector) column;
      int childCount = readMultiValuedLengths(list, destinationIndices, valueCount, repeating);
      list.child.ensureSize(childCount, false);
      readColumn(list.child, childCount);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      int childCount = readMultiValuedLengths(map, destinationIndices, valueCount, repeating);
      map.keys.ensureSize(childCount, false);
      map.values.ensureSize(childCount, false);
      readColumn(map.keys, childCount);
      readColumn(map.values, childCount);
    } else {
      readValue(column, destinationIndices, valueCount, repeating, nullBitmap, sourceIsDecimal64);
    }
  }

  private void readStructChildren(StructColumnVector struct, int[] destinationIndices,
      int valueCount, boolean repeating) throws IOException {
    int activeCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (!struct.isNull[destinationIndex(destinationIndices, logical, repeating)]) {
        activeCount++;
      }
    }

    int[] activeDestinationIndices = new int[activeCount];
    int activePosition = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      if (!struct.isNull[destinationIndex]) {
        activeDestinationIndices[activePosition++] = destinationIndex;
      }
    }

    for (ColumnVector field : struct.fields) {
      field.ensureSize(requiredSize(activeDestinationIndices, activeCount, false), false);
      readColumn(field, activeDestinationIndices, activeCount);
    }
  }

  private void readUnionChildren(UnionColumnVector union, int[] destinationIndices,
      int valueCount, boolean repeating) throws IOException {
    int[] fieldCounts = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      if (!union.isNull[destinationIndex]) {
        int tag = readInt();
        if (tag < 0 || tag >= union.fields.length) {
          throw new IOException("Invalid union tag " + tag + " for " + union.fields.length
              + " fields");
        }
        union.tags[destinationIndex] = tag;
        fieldCounts[tag]++;
      }
    }

    int[][] fieldDestinationIndices = new int[union.fields.length][];
    for (int tag = 0; tag < union.fields.length; tag++) {
      fieldDestinationIndices[tag] = new int[fieldCounts[tag]];
    }
    int[] fieldPositions = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      if (!union.isNull[destinationIndex]) {
        int tag = union.tags[destinationIndex];
        fieldDestinationIndices[tag][fieldPositions[tag]++] = destinationIndex;
      }
    }

    for (int tag = 0; tag < union.fields.length; tag++) {
      ColumnVector field = union.fields[tag];
      field.ensureSize(requiredSize(fieldDestinationIndices[tag], fieldCounts[tag], false), false);
      readColumn(field, fieldDestinationIndices[tag], fieldCounts[tag]);
    }
  }

  private int readMultiValuedLengths(MultiValuedColumnVector column, int[] destinationIndices,
      int valueCount, boolean repeating) throws IOException {
    int childCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      column.offsets[destinationIndex] = childCount;
      if (!column.isNull[destinationIndex]) {
        int length = readInt();
        if (length < 0) {
          throw new IOException("Negative multi-valued vector length " + length);
        }
        column.lengths[destinationIndex] = length;
        childCount = Math.addExact(childCount, length);
      } else {
        column.lengths[destinationIndex] = 0;
      }
    }
    column.childCount = childCount;
    return childCount;
  }

  private void requireAvailable(int bytes) throws IOException {
    if (position + bytes > limit) {
      throw new IOException("Vector shuffle batch ended while reading " + bytes + " bytes");
    }
  }

  private int readUnsignedByte() throws IOException {
    requireAvailable(1);
    return buffer[position++] & 0xff;
  }

  private boolean readBoolean() throws IOException {
    return readUnsignedByte() != 0;
  }

  private int readInt() throws IOException {
    requireAvailable(4);
    return ((buffer[position++] & 0xff) << 24)
        | ((buffer[position++] & 0xff) << 16)
        | ((buffer[position++] & 0xff) << 8)
        | (buffer[position++] & 0xff);
  }

  private long readLong() throws IOException {
    requireAvailable(8);
    return ((long) (buffer[position++] & 0xff) << 56)
        | ((long) (buffer[position++] & 0xff) << 48)
        | ((long) (buffer[position++] & 0xff) << 40)
        | ((long) (buffer[position++] & 0xff) << 32)
        | ((long) (buffer[position++] & 0xff) << 24)
        | ((long) (buffer[position++] & 0xff) << 16)
        | ((long) (buffer[position++] & 0xff) << 8)
        | ((long) buffer[position++] & 0xff);
  }

  private double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  private void readFully(byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  private void readFully(byte[] bytes, int offset, int length) throws IOException {
    requireAvailable(length);
    System.arraycopy(buffer, position, bytes, offset, length);
    position += length;
  }

  private boolean isNull(byte[] nullBitmap, int logical) {
    return nullBitmap != null && (nullBitmap[logical >>> 3] & (1 << (logical & 7))) != 0;
  }

  private int destinationIndex(int[] destinationIndices, int logical, boolean repeating) {
    return repeating ? 0 : destinationIndices == null ? logical : destinationIndices[logical];
  }

  private int requiredSize(int[] destinationIndices, int valueCount, boolean repeating) {
    if (repeating || destinationIndices == null) {
      return valueCount;
    }
    int requiredSize = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      requiredSize = Math.max(requiredSize, destinationIndices[logical] + 1);
    }
    return requiredSize;
  }

  private void readTypeMetadata(ColumnVector column) throws IOException {
    if (column instanceof DateColumnVector) {
      ((DateColumnVector) column).setUsingProlepticCalendar(readBoolean());
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      timestamp.setIsUTC(readBoolean());
      timestamp.setUsingProlepticCalendar(readBoolean());
    }
  }

  private void readValue(ColumnVector column, int[] destinationIndices, int valueCount,
      boolean repeating, byte[] nullBitmap, boolean sourceIsDecimal64) throws IOException {
    if (sourceIsDecimal64) {
      if (column instanceof Decimal64ColumnVector) {
        Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmap, logical)) {
            decimal64.vector[destinationIndex(destinationIndices, logical, repeating)] = readLong();
          }
        }
      } else if (column instanceof DecimalColumnVector) {
        DecimalColumnVector decimal = (DecimalColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmap, logical)) {
            decimal.vector[destinationIndex(destinationIndices, logical, repeating)]
                .deserialize64(readLong(), decimal.scale);
          }
        }
      } else {
        throw unsupported(column);
      }
      return;
    }
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytesColumn = (BytesColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          int length = readInt();
          if (length < 0) {
            throw new IOException("Negative byte-vector value length " + length);
          }
          byte[] bytes = new byte[length];
          readFully(bytes);
          bytesColumn.setVal(destinationIndex(destinationIndices, logical, repeating), bytes);
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          int index = destinationIndex(destinationIndices, logical, repeating);
          timestamp.time[index] = readLong();
          timestamp.nanos[index] = readInt();
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      IntervalDayTimeColumnVector interval = (IntervalDayTimeColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          interval.set(destinationIndex(destinationIndices, logical, repeating),
              new HiveIntervalDayTime(readLong(), readInt()));
        }
      }
    } else if (column instanceof Decimal64ColumnVector) {
      Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          HiveDecimalWritable decimal = new HiveDecimalWritable();
          position += decimal.readDirect(buffer, position);
          decimal64.set(destinationIndex(destinationIndices, logical, repeating), decimal);
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          position += decimal.vector[destinationIndex(destinationIndices, logical, repeating)]
              .readDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector extends LongColumnVector, so this branch covers it.
      LongColumnVector longs = (LongColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          longs.vector[destinationIndex(destinationIndices, logical, repeating)] = readLong();
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          doubles.vector[destinationIndex(destinationIndices, logical, repeating)] = readDouble();
        }
      }
    } else if (column instanceof VoidColumnVector) {
      // VOID has no value payload.
    } else {
      throw unsupported(column);
    }
  }

  private IllegalArgumentException unsupported(ColumnVector column) {
    return new IllegalArgumentException(
        "Unsupported vector shuffle column type " + column.getClass().getName());
  }
}
