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

  /** Inspects dimensions without resetting or changing the destination. */
  public int getRowCount(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    try {
      start(serialized);
      int rowCount = readInt();
      validateDimensions(destination, rowCount, readInt());
      return rowCount;
    } finally {
      buffer = null;
    }
  }

  /**
   * Validates the complete frame and checks batch-wide date/timestamp metadata before any prefix
   * can be changed. A false result requires the receiver to flush and use the replacement API for
   * this frame. Metadata is compared with the schema's ordinary-row representation, even when the
   * destination is empty, so ordinary and compact records can safely alternate.
   */
  public boolean isAppendCompatible(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    try {
      start(serialized);
      int rowCount = readInt();
      int columnCount = readInt();
      validateAppendDimensions(destination, rowCount, columnCount);
      boolean compatible = true;
      for (int i = 0; i < columnCount; i++) {
        compatible &= inspectColumn(destination.cols[i], rowCount);
      }
      checkEnd();
      return compatible;
    } finally {
      buffer = null;
    }
  }

  /**
   * Appends directly into destination-owned storage, preserving its dense, nonrepeating prefix.
   * Only successful decoding publishes the new size. A failure invalidates the destination; callers
   * must fail the task and discard it, rather than dispatching the partially decoded batch.
   */
  public void append(BytesWritable serialized, VectorizedRowBatch destination) throws IOException {
    if (destination.selectedInUse) {
      throw new IOException("Cannot append to a selected vector shuffle destination");
    }
    if (!isAppendCompatible(serialized, destination)) {
      throw new IOException("Incompatible vector shuffle date/timestamp metadata");
    }
    try {
      start(serialized);
      int rowCount = readInt();
      int columnCount = readInt();
      validateAppendCapacity(destination, rowCount);
      // The inspection above also validates empty frames, without changing any destination state.
      if (rowCount == 0) {
        return;
      }
      int[] indices = consecutiveIndices(destination.size, rowCount);
      for (int i = 0; i < columnCount; i++) {
        readAppendColumn(destination.cols[i], indices, rowCount);
      }
      checkEnd();
      destination.size += rowCount;
    } finally {
      buffer = null;
    }
  }

  private void start(BytesWritable serialized) throws IOException {
    if (serialized == null) {
      throw new IOException("Missing vector shuffle payload");
    }
    buffer = serialized.getBytesRaw();
    position = serialized.getOffset();
    int length = serialized.getLength();
    if (buffer == null || position < 0 || length < 0 || position > buffer.length - length) {
      throw new IOException("Invalid vector shuffle payload bounds");
    }
    limit = position + length;
  }

  private void validateDimensions(VectorizedRowBatch destination, int rowCount, int columnCount)
      throws IOException {
    if (destination == null || rowCount < 0 || columnCount < 0
        || columnCount > destination.cols.length) {
      throw new IOException("Invalid vector shuffle batch dimensions: " + rowCount + " rows, "
          + columnCount + " columns");
    }
    for (int i = 0; i < columnCount; i++) {
      if (destination.cols[i] == null) {
        throw new IOException("Destination column " + i + " is not initialized");
      }
    }
  }

  private void validateAppendDimensions(VectorizedRowBatch destination, int rowCount,
      int columnCount) throws IOException {
    validateDimensions(destination, rowCount, columnCount);
    if (columnCount != destination.projectionSize) {
      throw new IOException("Vector shuffle frame has " + columnCount + " columns; expected "
          + destination.projectionSize);
    }
    int capacity = appendCapacity(destination);
    if (rowCount > capacity) {
      throw new IOException("Vector shuffle frame of " + rowCount + " rows does not fit destination"
          + " capacity " + capacity);
    }
  }

  private void validateAppendCapacity(VectorizedRowBatch destination, int rowCount)
      throws IOException {
    int capacity = appendCapacity(destination);
    if (destination.size < 0 || destination.size > capacity - rowCount) {
      throw new IOException("Vector shuffle frame of " + rowCount + " rows does not fit destination"
          + " with " + destination.size + " pending rows and capacity " + capacity);
    }
  }

  private int appendCapacity(VectorizedRowBatch destination) {
    int capacity = Math.min(VectorizedRowBatch.DEFAULT_SIZE, destination.selected.length);
    for (int i = 0; i < destination.projectionSize; i++) {
      capacity = Math.min(capacity, destination.cols[i].isNull.length);
    }
    return capacity;
  }

  private void checkEnd() throws IOException {
    if (position != limit) {
      throw new IOException("Vector shuffle batch has " + (limit - position) + " trailing bytes");
    }
  }

  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    assert serialized != null && destination != null;
    try {
      start(serialized);
      final int rowCount = readInt();
      final int columnCount = readInt();
      validateDimensions(destination, rowCount, columnCount);

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
      checkEnd();
    } finally {
      buffer = null;
    }
  }

  private int[] consecutiveIndices(int offset, int count) throws IOException {
    checkedAdd(offset, count);
    int[] indices = new int[count];
    for (int i = 0; i < count; i++) {
      indices[i] = offset + i;
    }
    return indices;
  }

  private int checkedAdd(int first, int second) throws IOException {
    if (first < 0 || second < 0 || first > Integer.MAX_VALUE - second) {
      throw new IOException("Vector shuffle child count exceeds supported capacity");
    }
    return first + second;
  }

  private int readFlags(ColumnVector column) throws IOException {
    int flags = readUnsignedByte();
    if ((flags & ~(IS_REPEATING | HAS_NULLS | IS_DECIMAL_64)) != 0
        || ((flags & IS_DECIMAL_64) != 0 && !(column instanceof Decimal64ColumnVector)
            && !(column instanceof DecimalColumnVector))) {
      throw new IOException("Invalid vector shuffle flags " + flags + " for "
          + column.getClass().getSimpleName());
    }
    return flags;
  }

  private byte[] readNullBitmap(int flags, int valueCount) throws IOException {
    if ((flags & HAS_NULLS) == 0) {
      return null;
    }
    int length = (int) ((valueCount + 7L) / 8);
    requireBytes(length);
    byte[] bitmap = new byte[length];
    readFully(bitmap);
    return bitmap;
  }

  private boolean inspectColumn(ColumnVector column, int rowCount) throws IOException {
    int flags = readFlags(column);
    int count = (flags & IS_REPEATING) != 0 ? Math.min(rowCount, 1) : rowCount;
    byte[] nulls = readNullBitmap(flags, count);
    boolean compatible = inspectTypeMetadata(column, count);
    int nonNullCount = count;
    if (nulls != null) {
      for (int i = 0; i < count; i++) {
        if (isNull(nulls, i)) {
          nonNullCount--;
        }
      }
    }
    if (column instanceof StructColumnVector) {
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        compatible &= inspectColumn(field, nonNullCount);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < nonNullCount; i++) {
        int tag = readInt();
        validateUnionTag(tag, union.fields.length);
        counts[tag]++;
      }
      for (int tag = 0; tag < union.fields.length; tag++) {
        compatible &= inspectColumn(union.fields[tag], counts[tag]);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      int children = 0;
      for (int i = 0; i < nonNullCount; i++) {
        children = checkedAdd(children, readInt());
      }
      if (column instanceof ListColumnVector) {
        compatible &= inspectColumn(((ListColumnVector) column).child, children);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        compatible &= inspectColumn(map.keys, children);
        compatible &= inspectColumn(map.values, children);
      }
    } else if ((flags & IS_DECIMAL_64) != 0
        || (column instanceof LongColumnVector && !(column instanceof Decimal64ColumnVector))
        || column instanceof DoubleColumnVector) {
      skipFixedValues(nonNullCount, Long.BYTES);
    } else if (column instanceof BytesColumnVector) {
      for (int i = 0; i < nonNullCount; i++) {
        skipBytes(readInt());
      }
    } else if (column instanceof TimestampColumnVector
        || column instanceof IntervalDayTimeColumnVector) {
      skipFixedValues(nonNullCount, Long.BYTES + Integer.BYTES);
    } else if (column instanceof DecimalColumnVector || column instanceof Decimal64ColumnVector) {
      for (int i = 0; i < nonNullCount; i++) {
        skipBytes(decimalLength());
      }
    } else if (!(column instanceof VoidColumnVector)) {
      throw unsupported(column);
    }
    return compatible;
  }

  private boolean inspectTypeMetadata(ColumnVector column, int valueCount) throws IOException {
    if (column instanceof DateColumnVector) {
      boolean proleptic = readBoolean();
      return valueCount == 0
          || proleptic == ((DateColumnVector) column).isUsingProlepticCalendar();
    }
    if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      boolean utc = readBoolean();
      boolean proleptic = readBoolean();
      return valueCount == 0
          || utc == timestamp.isUTC() && proleptic == timestamp.usingProlepticCalendar();
    }
    return true;
  }

  private void skipFixedValues(int count, int width) throws IOException {
    if (count > (limit - position) / width) {
      throw new IOException("Truncated vector shuffle values");
    }
    skipBytes(count * width);
  }

  private void skipBytes(int length) throws IOException {
    requireBytes(length);
    position += length;
  }

  // HiveDecimalWritable's direct representation uses big-endian integers, unlike frame dimensions.
  private int decimalLength() throws IOException {
    requireBytes(2 * Integer.BYTES);
    int p = position + Integer.BYTES;
    int length = ((buffer[p] & 0xff) << 24) | ((buffer[p + 1] & 0xff) << 16)
        | ((buffer[p + 2] & 0xff) << 8) | (buffer[p + 3] & 0xff);
    if (length <= 0 || length > limit - position - 2 * Integer.BYTES) {
      throw new IOException("Invalid vector shuffle decimal length " + length);
    }
    return 2 * Integer.BYTES + length;
  }

  private void validateUnionTag(int tag, int fieldCount) throws IOException {
    if (tag < 0 || tag >= fieldCount) {
      throw new IOException("Invalid union tag " + tag + " for " + fieldCount + " fields");
    }
  }

  private void readAppendColumn(ColumnVector column, int[] indices, int rowCount)
      throws IOException {
    assert !column.isRepeating;
    int flags = readFlags(column);
    boolean repeating = (flags & IS_REPEATING) != 0;
    int count = repeating ? Math.min(rowCount, 1) : rowCount;
    column.ensureSize(requiredSize(indices, rowCount, false), true);
    byte[] nulls = readNullBitmap(flags, count);
    for (int i = 0; i < count; i++) {
      boolean isNull = isNull(nulls, i);
      column.isNull[indices[i]] = isNull;
      if (isNull) {
        column.noNulls = false;
      }
    }
    // Compatibility was checked before decoding. Empty nested columns leave metadata untouched.
    if (!inspectTypeMetadata(column, count)) {
      throw new IOException("Incompatible vector shuffle date/timestamp metadata");
    }
    if (column instanceof StructColumnVector) {
      int activeCount = 0;
      for (int i = 0; i < count; i++) {
        if (!isNull(nulls, i)) {
          activeCount++;
        }
      }
      int[] active = new int[activeCount];
      int next = 0;
      for (int i = 0; i < count; i++) {
        if (!isNull(nulls, i)) {
          active[next++] = indices[i];
        }
      }
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        readAppendColumn(field, active, activeCount);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < count; i++) {
        if (!isNull(nulls, i)) {
          int tag = readInt();
          validateUnionTag(tag, union.fields.length);
          union.tags[indices[i]] = tag;
          counts[tag]++;
        }
      }
      int[][] active = new int[union.fields.length][];
      int[] next = new int[union.fields.length];
      for (int tag = 0; tag < union.fields.length; tag++) {
        active[tag] = new int[counts[tag]];
      }
      for (int i = 0; i < count; i++) {
        if (!isNull(nulls, i)) {
          int tag = union.tags[indices[i]];
          active[tag][next[tag]++] = indices[i];
        }
      }
      for (int tag = 0; tag < union.fields.length; tag++) {
        readAppendColumn(union.fields[tag], active[tag], counts[tag]);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      MultiValuedColumnVector multi = (MultiValuedColumnVector) column;
      int oldChildCount = multi.childCount;
      int childCount = oldChildCount;
      for (int i = 0; i < count; i++) {
        int index = indices[i];
        multi.offsets[index] = childCount;
        int length = isNull(nulls, i) ? 0 : readInt();
        childCount = checkedAdd(childCount, length);
        multi.lengths[index] = length;
      }
      int incomingChildren = childCount - oldChildCount;
      int[] children = consecutiveIndices(oldChildCount, incomingChildren);
      if (column instanceof ListColumnVector) {
        readAppendColumn(((ListColumnVector) column).child, children, incomingChildren);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        readAppendColumn(map.keys, children, incomingChildren);
        readAppendColumn(map.values, children, incomingChildren);
      }
      multi.childCount = childCount;
    } else {
      // The source repeating flag must never redirect writes to the accumulator's row zero.
      readValue(column, indices, count, false, nulls, (flags & IS_DECIMAL_64) != 0);
    }
    if (repeating && rowCount > 1) {
      for (int i = 1; i < rowCount; i++) {
        copyRepeatedElement(column, indices[0], indices[i]);
      }
    }
  }

  private void copyRepeatedElement(ColumnVector column, int source, int destination) {
    column.isNull[destination] = column.isNull[source];
    if (column.isNull[source]) {
      column.noNulls = false;
      return;
    }
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytes = (BytesColumnVector) column;
      // References are safe here: the original value was copied into destination-owned storage.
      bytes.setRef(destination, bytes.vector[source], bytes.start[source], bytes.length[source]);
    } else if (column instanceof MultiValuedColumnVector) {
      MultiValuedColumnVector multi = (MultiValuedColumnVector) column;
      // As in MultiValuedColumnVector.flatten(), repeated rows can share a decoded child range.
      multi.offsets[destination] = multi.offsets[source];
      multi.lengths[destination] = multi.lengths[source];
    } else if (column instanceof StructColumnVector) {
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        copyRepeatedElement(field, source, destination);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int tag = union.tags[source];
      union.tags[destination] = tag;
      copyRepeatedElement(union.fields[tag], source, destination);
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimals = (DecimalColumnVector) column;
      // Preserve the decoded value exactly, as readDirect/deserialize64 do above.
      decimals.vector[destination].set(decimals.vector[source]);
    } else if (!(column instanceof VoidColumnVector)) {
      column.setElement(destination, source, column);
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
    final int valueCount = repeating ? Math.min(rowCount, 1) : rowCount;
    column.ensureSize(requiredSize(destinationIndices, valueCount, repeating), false);

    byte[] nullBitmap = readNullBitmap(flags, valueCount);

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
        childCount = checkedAdd(childCount, length);
      } else {
        column.lengths[destinationIndex] = 0;
      }
    }
    column.childCount = childCount;
    return childCount;
  }

  private void requireBytes(int count) throws IOException {
    if (count < 0 || count > limit - position) {
      throw new IOException("Truncated vector shuffle payload at byte " + position
          + ": need " + count + " bytes, remaining " + (limit - position));
    }
  }

  private int readUnsignedByte() throws IOException {
    requireBytes(Byte.BYTES);
    return buffer[position++] & 0xff;
  }

  private boolean readBoolean() throws IOException {
    int value = readUnsignedByte();
    if (value > 1) {
      throw new IOException("Invalid vector shuffle boolean " + value);
    }
    return value != 0;
  }

  private int readInt() throws IOException {
    requireBytes(Integer.BYTES);
    int value = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Integer.BYTES;
    return value;
  }

  private long readLong() throws IOException {
    requireBytes(Long.BYTES);
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
    requireBytes(length);
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
          requireBytes(length);
          bytesColumn.setVal(destinationIndex(destinationIndices, logical, repeating),
              buffer, position, length);
          position += length;
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
          decimalLength();
          position += decimal.readDirect(buffer, position);
          decimal64.set(destinationIndex(destinationIndices, logical, repeating), decimal);
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          decimalLength();
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
