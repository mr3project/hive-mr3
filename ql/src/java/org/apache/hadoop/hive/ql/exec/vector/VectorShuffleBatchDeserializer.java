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

  /** Validated framing information, without transferring any ownership of the input bytes. */
  public static final class FrameInfo {
    public final int rowCount;
    public final boolean compatible;

    private FrameInfo(int rowCount, boolean compatible) {
      this.rowCount = rowCount;
      this.compatible = compatible;
    }
  }

  private boolean compatible;

  private void setInput(BytesWritable serialized) throws IOException {
    if (serialized == null) {
      throw new IOException("Missing vector shuffle payload");
    }
    buffer = serialized.getBytesRaw();
    int offset = serialized.getOffset();
    int length = serialized.getLength();
    if (buffer == null || offset < 0 || length < 0 || offset > buffer.length - length) {
      throw new IOException("Invalid vector shuffle payload bounds");
    }
    position = offset;
    limit = offset + length;
  }

  private int[] readDimensions(VectorizedRowBatch destination) throws IOException {
    int rows = readInt();
    int columns = readInt();
    if (rows < 0 || columns < 0 || columns > destination.cols.length) {
      throw new IOException("Invalid vector shuffle batch dimensions: " + rows + " rows, "
          + columns + " columns");
    }
    return new int[] { rows, columns };
  }

  /**
   * Validate an entire frame before the receiver modifies its pending batch. In particular,
   * check nested metadata so that date/timestamp representations can be kept in separate batches.
   * The scan reads input bytes in place; it does not deserialize into an intermediate batch.
   */
  public FrameInfo inspect(BytesWritable serialized, VectorizedRowBatch destination,
      int expectedColumns) throws IOException {
    try {
      setInput(serialized);
      int[] dimensions = readDimensions(destination);
      int rowCount = dimensions[0];
      int columnCount = dimensions[1];
      int maxRows = Math.min(VectorizedRowBatch.DEFAULT_SIZE, destination.getMaxSize());
      if (rowCount > maxRows) {
        throw new IOException("Vector shuffle frame has " + rowCount
            + " rows; receiver limit is " + maxRows);
      }
      if (columnCount != expectedColumns) {
        throw new IOException("Unexpected vector shuffle column count " + columnCount
            + ", expected " + expectedColumns);
      }
      compatible = true;
      for (int i = 0; i < columnCount; i++) {
        if (destination.cols[i] == null) {
          throw new IOException("Destination column " + i + " is not initialized");
        }
        scanColumn(destination.cols[i], rowCount, destination.size > 0);
      }
      if (position != limit) {
        throw new IOException("Vector shuffle batch has " + (limit - position)
            + " trailing bytes");
      }
      return new FrameInfo(rowCount, compatible);
    } finally {
      buffer = null;
    }
  }

  // This validation path also consumes the metadata for empty nested children. Their
  // incoming flags must never change metadata belonging to an existing prefix.
  private void scanColumn(ColumnVector column, int count, boolean compareMetadata)
      throws IOException {
    int flags = readUnsignedByte();
    if ((flags & ~(IS_REPEATING | HAS_NULLS | IS_DECIMAL_64)) != 0) {
      throw new IOException("Unsupported vector shuffle flags " + flags);
    }
    boolean repeating = (flags & IS_REPEATING) != 0;
    if ((flags & IS_DECIMAL_64) != 0
        && !(column instanceof Decimal64ColumnVector || column instanceof DecimalColumnVector)) {
      throw new IOException("Invalid decimal64 vector shuffle representation");
    }
    int valueCount = repeating ? Math.min(count, 1) : count;
    int bitmapOffset = -1;
    if ((flags & HAS_NULLS) != 0) {
      bitmapOffset = position;
      skipBytes((valueCount + 7) / 8);
    }
    if (column instanceof DateColumnVector) {
      boolean calendar = readBoolean();
      if (compareMetadata && ((DateColumnVector) column).isUsingProlepticCalendar() != calendar) {
        compatible = false;
      }
    } else if (column instanceof TimestampColumnVector) {
      boolean utc = readBoolean();
      boolean calendar = readBoolean();
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      if (compareMetadata && (timestamp.isUTC() != utc
          || timestamp.usingProlepticCalendar() != calendar)) {
        compatible = false;
      }
    }

    if (column instanceof StructColumnVector) {
      int active = nonNullCount(bitmapOffset, valueCount);
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        scanColumn(field, active, compareMetadata);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < valueCount; i++) {
        if (!bitmapNull(bitmapOffset, i)) {
          int tag = readInt();
          if (tag < 0 || tag >= counts.length) {
            throw new IOException("Invalid vector shuffle union tag " + tag);
          }
          counts[tag]++;
        }
      }
      for (int tag = 0; tag < counts.length; tag++) {
        scanColumn(union.fields[tag], counts[tag], compareMetadata);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      int childCount = 0;
      for (int i = 0; i < valueCount; i++) {
        if (!bitmapNull(bitmapOffset, i)) {
          int length = readInt();
          if (length < 0 || length > Integer.MAX_VALUE - childCount) {
            throw new IOException("Invalid multi-valued vector length " + length);
          }
          childCount += length;
        }
      }
      if (column instanceof ListColumnVector) {
        scanColumn(((ListColumnVector) column).child, childCount, compareMetadata);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        scanColumn(map.keys, childCount, compareMetadata);
        scanColumn(map.values, childCount, compareMetadata);
      }
    } else {
      boolean decimal64 = (flags & IS_DECIMAL_64) != 0;
      if (decimal64 && !(column instanceof Decimal64ColumnVector
          || column instanceof DecimalColumnVector)) {
        throw new IOException("Invalid decimal64 vector shuffle representation");
      }
      for (int i = 0; i < valueCount; i++) {
        if (bitmapNull(bitmapOffset, i)) {
          continue;
        }
        if (decimal64) {
          skipBytes(Long.BYTES);
        } else if (column instanceof BytesColumnVector) {
          int length = readInt();
          if (length < 0) {
            throw new IOException("Negative byte-vector value length " + length);
          }
          skipBytes(length);
        } else if (column instanceof TimestampColumnVector
            || column instanceof IntervalDayTimeColumnVector) {
          skipBytes(Long.BYTES + Integer.BYTES);
        } else if (column instanceof DecimalColumnVector
            || column instanceof Decimal64ColumnVector) {
          // HiveDecimalWritable.writeDirect writes these integers in big-endian order,
          // unlike the native-endian vector frame fields.
          skipBytes(Integer.BYTES);
          int length = readBigEndianInt();
          if (length < 0) {
            throw new IOException("Negative decimal byte length " + length);
          }
          skipBytes(length);
        } else if (column instanceof LongColumnVector
            || column instanceof DoubleColumnVector) {
          skipBytes(Long.BYTES);
        } else if (!(column instanceof VoidColumnVector)) {
          throw new IOException("Unsupported vector shuffle column " + column.getClass());
        }
      }
    }
  }

  private int nonNullCount(int bitmapOffset, int count) {
    int result = 0;
    for (int i = 0; i < count; i++) {
      if (!bitmapNull(bitmapOffset, i)) {
        result++;
      }
    }
    return result;
  }

  private boolean bitmapNull(int bitmapOffset, int logical) {
    return bitmapOffset >= 0
        && (buffer[bitmapOffset + (logical >>> 3)] & (1 << (logical & 7))) != 0;
  }

  private void requireAvailable(int size) throws IOException {
    if (size < 0 || position > limit - size) {
      throw new IOException("Truncated vector shuffle batch at byte " + position
          + ", requested " + size + ", limit " + limit);
    }
  }

  private int readBigEndianInt() throws IOException {
    requireAvailable(Integer.BYTES);
    int value = ((buffer[position] & 0xff) << 24)
        | ((buffer[position + 1] & 0xff) << 16)
        | ((buffer[position + 2] & 0xff) << 8)
        | (buffer[position + 3] & 0xff);
    position += Integer.BYTES;
    return value;
  }

  private void skipBytes(int size) throws IOException {
    requireAvailable(size);
    position += size;
  }

  /**
   * Append a previously inspected frame directly into a dense, nonrepeating destination.
   * The caller publishes destination.size only after this method returns successfully.
   */
  public void append(BytesWritable serialized, VectorizedRowBatch destination, int rowCount,
      int expectedColumns) throws IOException {
    int startRow = destination.size;
    if (startRow < 0 || rowCount < 0 || rowCount > destination.getMaxSize() - startRow) {
      throw new IOException("Vector shuffle frame exceeds remaining destination capacity");
    }
    try {
      setInput(serialized);
      int[] dimensions = readDimensions(destination);
      if (dimensions[0] != rowCount || dimensions[1] != expectedColumns) {
        throw new IOException("Vector shuffle frame changed since inspection");
      }
      if (destination.selectedInUse) {
        throw new IOException("Cannot append to a selected vector shuffle batch");
      }
      for (int i = 0; i < expectedColumns; i++) {
        appendColumn(destination.cols[i], null, rowCount, startRow);
      }
      if (position != limit) {
        throw new IOException("Vector shuffle batch has " + (limit - position)
            + " trailing bytes");
      }
      destination.size = startRow + rowCount;
    } finally {
      buffer = null;
    }
  }

  /** True if a compact frame left batch-wide representation metadata unsuitable for ordinary rows. */
  public boolean hasNonDefaultMetadata(VectorizedRowBatch destination, int columnCount) {
    for (int i = 0; i < columnCount; i++) {
      if (hasNonDefaultMetadata(destination.cols[i])) {
        return true;
      }
    }
    return false;
  }

  private boolean hasNonDefaultMetadata(ColumnVector column) {
    if (column instanceof DateColumnVector) {
      return ((DateColumnVector) column).isUsingProlepticCalendar();
    }
    if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      return timestamp.isUTC() || timestamp.usingProlepticCalendar();
    }
    if (column instanceof StructColumnVector) {
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        if (hasNonDefaultMetadata(field)) {
          return true;
        }
      }
    } else if (column instanceof UnionColumnVector) {
      for (ColumnVector field : ((UnionColumnVector) column).fields) {
        if (hasNonDefaultMetadata(field)) {
          return true;
        }
      }
    } else if (column instanceof ListColumnVector) {
      return hasNonDefaultMetadata(((ListColumnVector) column).child);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      return hasNonDefaultMetadata(map.keys) || hasNonDefaultMetadata(map.values);
    }
    return false;
  }

  private int appendIndex(int[] destinationIndices, int logical, int base) {
    return destinationIndices == null ? base + logical : destinationIndices[logical];
  }

  private int appendRequiredSize(int[] destinationIndices, int count, int base) {
    if (destinationIndices == null) {
      return base + count;
    }
    int needed = 0;
    for (int i = 0; i < count; i++) {
      needed = Math.max(needed, destinationIndices[i] + 1);
    }
    return needed;
  }

  private void appendColumn(ColumnVector column, int[] destinationIndices, int count,
      int base) throws IOException {
    if (count == 0) {
      // Still consume and validate its serialized subtree without touching prior child data.
      scanColumn(column, 0, false);
      return;
    }
    int flags = readUnsignedByte();
    boolean repeating = (flags & IS_REPEATING) != 0;
    boolean hasNulls = (flags & HAS_NULLS) != 0;
    int valueCount = repeating ? 1 : count;
    column.ensureSize(appendRequiredSize(destinationIndices, count, base), true);

    byte[] nullBitmap = null;
    if (hasNulls) {
      nullBitmap = new byte[(valueCount + 7) / 8];
      readFully(nullBitmap);
      column.noNulls = false;
    }
    for (int logical = 0; logical < valueCount; logical++) {
      column.isNull[appendIndex(destinationIndices, logical, base)] =
          hasNulls && isNull(nullBitmap, logical);
    }
    // Frame-local repeating does not become a destination-wide repeating flag.
    column.isRepeating = false;
    readTypeMetadata(column);

    if (column instanceof StructColumnVector) {
      StructColumnVector struct = (StructColumnVector) column;
      int active = 0;
      for (int i = 0; i < valueCount; i++) {
        if (!isNull(nullBitmap, i)) {
          active++;
        }
      }
      int[] indices = new int[active];
      int next = 0;
      for (int i = 0; i < valueCount; i++) {
        if (!isNull(nullBitmap, i)) {
          indices[next++] = appendIndex(destinationIndices, i, base);
        }
      }
      for (ColumnVector field : struct.fields) {
        appendColumn(field, indices, active, 0);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < valueCount; i++) {
        if (!isNull(nullBitmap, i)) {
          int tag = readInt();
          if (tag < 0 || tag >= counts.length) {
            throw new IOException("Invalid union tag " + tag);
          }
          int index = appendIndex(destinationIndices, i, base);
          union.tags[index] = tag;
          counts[tag]++;
        }
      }
      int[][] indices = new int[counts.length][];
      for (int tag = 0; tag < counts.length; tag++) {
        indices[tag] = new int[counts[tag]];
      }
      int[] next = new int[counts.length];
      for (int i = 0; i < valueCount; i++) {
        if (!isNull(nullBitmap, i)) {
          int index = appendIndex(destinationIndices, i, base);
          int tag = union.tags[index];
          indices[tag][next[tag]++] = index;
        }
      }
      for (int tag = 0; tag < counts.length; tag++) {
        appendColumn(union.fields[tag], indices[tag], counts[tag], 0);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      MultiValuedColumnVector multi = (MultiValuedColumnVector) column;
      int oldChildCount = multi.childCount;
      int nextChild = oldChildCount;
      for (int i = 0; i < valueCount; i++) {
        int index = appendIndex(destinationIndices, i, base);
        multi.offsets[index] = nextChild;
        if (isNull(nullBitmap, i)) {
          multi.lengths[index] = 0;
        } else {
          int length = readInt();
          if (length < 0 || length > Integer.MAX_VALUE - nextChild) {
            throw new IOException("Invalid multi-valued vector length " + length);
          }
          multi.lengths[index] = length;
          nextChild += length;
        }
      }
      multi.childCount = nextChild;
      int newChildren = nextChild - oldChildCount;
      if (column instanceof ListColumnVector) {
        appendColumn(((ListColumnVector) column).child, null, newChildren, oldChildCount);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        appendColumn(map.keys, null, newChildren, oldChildCount);
        appendColumn(map.values, null, newChildren, oldChildCount);
      }
    } else {
      readValue(column, destinationIndices, valueCount, repeating, nullBitmap,
          (flags & IS_DECIMAL_64) != 0, true, base);
    }

    // Materialize this frame's repeating value at each destination row. setElement
    // recursively duplicates list/map children into destination-owned storage.
    if (repeating && count > 1) {
      int first = appendIndex(destinationIndices, 0, base);
      if (!column.isNull[first] && column instanceof MultiValuedColumnVector) {
        MultiValuedColumnVector multi = (MultiValuedColumnVector) column;
        long additionalChildren = (long) (count - 1) * multi.lengths[first];
        if (additionalChildren > Integer.MAX_VALUE - multi.childCount) {
          throw new IOException("Repeating vector children exceed destination capacity");
        }
      }
      for (int i = 1; i < count; i++) {
        int index = appendIndex(destinationIndices, i, base);
        if (column.isNull[first]) {
          column.isNull[index] = true;
          column.noNulls = false;
        } else {
          column.isNull[index] = false;
          column.setElement(index, first, column);
        }
      }
    }
  }

  // The Tez shuffle layer has already verified that the right number of bytes have been received,
  // so we usually do not expect ArrayIndexOutOfBoundsException.
  // IOException is for handling in-transit data corruption.
  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    assert serialized != null && destination != null;

    setInput(serialized);
    int[] dimensions = readDimensions(destination);
    final int rowCount = dimensions[0];
    final int columnCount = dimensions[1];

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

    buffer = null;
  }

  private void readColumn(ColumnVector column, int rowCount) throws IOException {
    readColumn(column, null, rowCount);
  }

  private void readColumn(ColumnVector column, int[] destinationIndices, int rowCount)
      throws IOException {
    final int flags = readUnsignedByte();
    final boolean repeating = (flags & IS_REPEATING) != 0;
    final boolean hasNulls = (flags & HAS_NULLS) != 0;
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
      final boolean sourceIsDecimal64 = (flags & IS_DECIMAL_64) != 0;
      readValue(column, destinationIndices, valueCount, repeating, nullBitmap,
          sourceIsDecimal64, false, 0);
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
          throw new IOException("Invalid union tag " + tag + " for " + union.fields.length + " fields");
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

  private int readUnsignedByte() throws IOException {
    requireAvailable(1);
    return buffer[position++] & 0xff;
  }

  private boolean readBoolean() throws IOException {
    return readUnsignedByte() != 0;
  }

  private int readInt() throws IOException {
    requireAvailable(Integer.BYTES);
    int value = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Integer.BYTES;
    return value;
  }

  private long readLong() throws IOException {
    requireAvailable(Long.BYTES);
    long value = theUnsafe.getLong(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Long.BYTES;
    return value;
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

  private int valueIndex(int[] destinationIndices, int logical, boolean repeating,
      boolean append, int base) {
    return append ? appendIndex(destinationIndices, logical, base)
        : destinationIndex(destinationIndices, logical, repeating);
  }

  private void readValue(ColumnVector column, int[] destinationIndices, int valueCount,
      boolean repeating, byte[] nullBitmap, boolean sourceIsDecimal64,
      boolean append, int base) throws IOException {
    if (sourceIsDecimal64) {
      if (column instanceof Decimal64ColumnVector) {
        Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmap, logical)) {
            decimal64.vector[valueIndex(destinationIndices, logical, repeating, append, base)] = readLong();
          }
        }
      } else if (column instanceof DecimalColumnVector) {
        DecimalColumnVector decimal = (DecimalColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmap, logical)) {
            decimal.vector[valueIndex(destinationIndices, logical, repeating, append, base)]
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
          bytesColumn.setVal(valueIndex(destinationIndices, logical, repeating, append, base),
              buffer, position, length);
          position += length;
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          int index = valueIndex(destinationIndices, logical, repeating, append, base);
          timestamp.time[index] = readLong();
          timestamp.nanos[index] = readInt();
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      IntervalDayTimeColumnVector interval = (IntervalDayTimeColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          interval.set(valueIndex(destinationIndices, logical, repeating, append, base),
              new HiveIntervalDayTime(readLong(), readInt()));
        }
      }
    } else if (column instanceof Decimal64ColumnVector) {
      Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          HiveDecimalWritable decimal = new HiveDecimalWritable();
          position += decimal.readDirect(buffer, position);
          decimal64.set(valueIndex(destinationIndices, logical, repeating, append, base), decimal);
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          position += decimal.vector[valueIndex(destinationIndices, logical, repeating, append, base)]
              .readDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector extends LongColumnVector, so this branch covers it.
      LongColumnVector longs = (LongColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          longs.vector[valueIndex(destinationIndices, logical, repeating, append, base)] = readLong();
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          doubles.vector[valueIndex(destinationIndices, logical, repeating, append, base)] = readDouble();
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
