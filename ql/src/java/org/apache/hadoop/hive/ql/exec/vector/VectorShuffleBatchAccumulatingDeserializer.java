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
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;

/** Deserializes trusted compact vector shuffle frames into a schema-initialized batch. */
public final class VectorShuffleBatchAccumulatingDeserializer {

  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;
  private static final int IS_DECIMAL_64 = 4;

  private byte[] buffer;
  private int position;
  private int limit;

  // Fixed-schema accumulation always uses batches created by the same VectorizedRowBatchCtx.
  private final int fixedAppendCapacity;
  private final int inputColumnCount;
  private final VectorShuffleBatchHeader header = new VectorShuffleBatchHeader();
  private final byte[] accumulatorRepeatingBitmap;
  private final byte[] accumulatorMetadata;

  private VectorShuffleBatchMetadata metadata;

  private long accumulatorRepeatingBits;
  private byte[] repeatingSection;
  private boolean hasRepeatingColumns;

  /** Cache the fixed input schema's capacity and header sizes, excluding scratch columns. */
  public VectorShuffleBatchAccumulatingDeserializer(VectorizedRowBatch schema, int inputColumnCount) {
    assert inputColumnCount >= 0 && inputColumnCount <= schema.cols.length;
    this.inputColumnCount = inputColumnCount;
    this.fixedAppendCapacity = calculateAppendCapacity(schema, inputColumnCount);
    this.accumulatorRepeatingBitmap =
        inputColumnCount <= Long.SIZE ? null : new byte[(inputColumnCount + 7) / 8];
    this.metadata = new VectorShuffleBatchMetadata(schema.cols, inputColumnCount);
    this.accumulatorMetadata = new byte[metadata.getByteCount()];
  }

  /** Row limit shared with the fixed-schema accumulation receiver. */
  public int getAppendCapacity() {
    return fixedAppendCapacity;
  }

  public boolean hasInputTypeMetadata() {
    return metadata.getBitCount() != 0;
  }

  public boolean hasRepeatingColumns() {
    return hasRepeatingColumns;
  }

  /** Forget pending summaries without invalidating a header read before a flush. */
  public void resetAccumulation() {
    // The first compact append overwrites summary bytes before any compatibility comparison.
    accumulatorRepeatingBits = 0;
    repeatingSection = null;
    hasRepeatingColumns = false;
  }

  /** Read only the fixed-size header; no column payload is visited. */
  public VectorShuffleBatchHeader readHeader(BytesWritable serialized) throws IOException {
    header.read(serialized, inputColumnCount, metadata.getByteCount());
    return header;
  }

  /** Release a borrowed incoming buffer if processing ends before append or standalone decode. */
  public void releaseHeader() {
    header.clear();
  }

  /**
   * Compare summaries before examining repeated bytes. Under the same schema, bitmap and metadata,
   * identical complete repeating encodings decode to identical representative values.
   */
  public boolean isCompatible() {
    if (hasRepeatingColumns != header.hasRepeatingColumns()) {
      return false;
    }

    byte[] incoming = header.getBuffer();

    if (hasRepeatingColumns) {
      if (inputColumnCount <= Long.SIZE) {
        if (accumulatorRepeatingBits != header.getRepeatingBits()) {
          return false;
        }
      } else if (!Arrays.equals(accumulatorRepeatingBitmap, 0, accumulatorRepeatingBitmap.length,
            incoming, header.getBitmapOffset(), header.getBitmapOffset() + header.getBitmapLength())) {
        return false;
      }
    }

    if (accumulatorMetadata.length != 0 &&
        !Arrays.equals(accumulatorMetadata, 0, accumulatorMetadata.length,
            incoming, header.getMetadataOffset(), header.getMetadataOffset() + header.getMetadataLength())) {
      return false;
    }

    if (!hasRepeatingColumns) {
      return true;
    }

    int length = header.getRepeatingEnd() - header.getRepeatingStart();
    return repeatingSection != null && repeatingSection.length == length &&
        Arrays.equals(repeatingSection, 0, length,
            incoming, header.getRepeatingStart(), header.getRepeatingEnd());
  }

  /**
   * Append a frame whose header has already been read and whose compatibility has been checked.
   * Repeating columns are decoded once at batch start; later frames jump directly to ordinary
   * columns. No offsets or lengths for individual columns are needed.
   */
  public void append(BytesWritable serialized, VectorizedRowBatch destination) throws IOException {
    assert header.getBuffer() == serialized.getBytesRaw();
    assert !header.hasNestedRepeating() && !destination.selectedInUse;
    assert destination.projectionSize == inputColumnCount;

    try {
      buffer = header.getBuffer();
      limit = header.getLimit();
      final int originalRows = destination.size;
      final int rowCount = header.getRowCount();
      assert originalRows <= fixedAppendCapacity - rowCount;

      if (originalRows == 0) {
        metadata.apply(destination.cols, buffer, header.getMetadataOffset());
        position = header.getRepeatingStart();
        if (header.hasRepeatingColumns()) {
          readRepeatingColumns(destination, inputColumnCount, rowCount);
        }
        checkRepeatingSectionEnd();
      }

      position = header.getOrdinaryStart();
      if (!header.hasRepeatingColumns()) {
        for (int i = 0; i < inputColumnCount; i++) {
          readAppendColumn(destination.cols[i], null, originalRows, rowCount);
        }
      } else if (inputColumnCount <= Long.SIZE) {
        long ordinaryBits = ~header.getRepeatingBits() & columnMask(inputColumnCount);
        while (ordinaryBits != 0) {
          int i = Long.numberOfTrailingZeros(ordinaryBits);
          readAppendColumn(destination.cols[i], null, originalRows, rowCount);
          ordinaryBits &= ordinaryBits - 1;
        }
      } else {
        for (int i = 0; i < inputColumnCount; i++) {
          if (!header.isRepeating(i)) {
            readAppendColumn(destination.cols[i], null, originalRows, rowCount);
          }
        }
      }

      checkFrameEnd();

      destination.size += rowCount;
      if (originalRows == 0) {
        if (accumulatorMetadata.length != 0) {
          System.arraycopy(buffer, header.getMetadataOffset(), accumulatorMetadata,
              0, accumulatorMetadata.length);
        }
        hasRepeatingColumns = header.hasRepeatingColumns();
        if (hasRepeatingColumns) {
          if (inputColumnCount <= Long.SIZE) {
            accumulatorRepeatingBits = header.getRepeatingBits();
          } else {
            System.arraycopy(buffer, header.getBitmapOffset(), accumulatorRepeatingBitmap,
                0, accumulatorRepeatingBitmap.length);
          }
          // The shuffle reader may reuse the incoming BytesWritable immediately after this call.
          repeatingSection = Arrays.copyOfRange(buffer, header.getRepeatingStart(), header.getRepeatingEnd());
        }
      }
    } finally {
      buffer = null;
      header.clear();
    }
  }

  private static long columnMask(int columnCount) {
    return -1L >>> (Long.SIZE - columnCount);
  }

  private void readRepeatingColumns(VectorizedRowBatch destination, int columnCount, int rowCount)
      throws IOException {
    if (columnCount <= Long.SIZE) {
      long repeatingBits = header.getRepeatingBits() & columnMask(columnCount);
      while (repeatingBits != 0) {
        int i = Long.numberOfTrailingZeros(repeatingBits);
        readColumn(destination.cols[i], rowCount);
        repeatingBits &= repeatingBits - 1;
      }
    } else {
      for (int i = 0; i < columnCount; i++) {
        if (header.isRepeating(i)) {
          readColumn(destination.cols[i], rowCount);
        }
      }
    }
  }

  private void checkRepeatingSectionEnd() throws IOException {
    if (position != header.getRepeatingEnd()) {
      throw new IOException("Vector shuffle repeating section length does not match its payload");
    }
  }

  private void checkFrameEnd() throws IOException {
    if (position != limit) {
      throw new IOException("Vector shuffle batch has " + (limit - position) + " trailing bytes");
    }
  }

  private static int calculateAppendCapacity(VectorizedRowBatch batch, int columnCount) {
    int capacity = Math.min(VectorizedRowBatch.DEFAULT_SIZE, batch.selected.length);
    for (int i = 0; i < columnCount; i++) {
      capacity = Math.min(capacity, batch.cols[i].isNull.length);
    }
    return capacity;
  }

  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    try {
      header.read(serialized, inputColumnCount, metadata.getByteCount());
    } catch (IOException e) {
      header.clear();
      throw e;
    }
    deserializeWithHeader(serialized, destination);
  }

  /** Decode a standalone frame using the header already read by the receiver. */
  public void deserializeWithHeader(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    assert header.getBuffer() == serialized.getBytesRaw();

    final int columnCount = header.getColumnCount();
    try {
      buffer = header.getBuffer();
      limit = header.getLimit();
      final int rowCount = header.getRowCount();

      destination.reset();

      for (int i = 0; i < columnCount; i++) {
        destination.cols[i].ensureSize(rowCount, false);
        destination.projectedColumns[i] = i;
      }
      destination.size = rowCount;
      destination.selectedInUse = false;
      destination.projectionSize = columnCount;
      metadata.apply(destination.cols, buffer, header.getMetadataOffset());
      position = header.getRepeatingStart();
      if (header.hasRepeatingColumns()) {
        readRepeatingColumns(destination, columnCount, rowCount);
      }

      checkRepeatingSectionEnd();

      position = header.getOrdinaryStart();
      if (!header.hasRepeatingColumns()) {
        for (int i = 0; i < columnCount; i++) {
          readColumn(destination.cols[i], rowCount);
        }
      } else if (columnCount <= Long.SIZE) {
        long ordinaryBits = ~header.getRepeatingBits() & columnMask(columnCount);
        while (ordinaryBits != 0) {
          int i = Long.numberOfTrailingZeros(ordinaryBits);
          readColumn(destination.cols[i], rowCount);
          ordinaryBits &= ordinaryBits - 1;
        }
      } else {
        for (int i = 0; i < columnCount; i++) {
          if (!header.isRepeating(i)) {
            readColumn(destination.cols[i], rowCount);
          }
        }
      }
      checkFrameEnd();
    } finally {
      buffer = null;
      header.clear();
    }
  }

  private int readNullBitmap(int flags, int valueCount) {
    if ((flags & HAS_NULLS) == 0) {
      return -1;
    }
    int bitmapOffset = position;
    position += (int) ((valueCount + 7L) / 8);
    return bitmapOffset;
  }

  private void readAppendColumnConsecutive(ColumnVector column, int offset, int rowCount)
      throws IOException {
    // Nested storage grows independently of the top-level accumulator's fixed row capacity.
    column.ensureSize(offset + rowCount, true);
    readAppendColumn(column, null, offset, rowCount);
  }

  // A reused vector may retain ignored null bits when noNulls is true.
  private void readNulls(ColumnVector column, int[] indices, int offset, int count,
      boolean repeating, int bitmapOffset) {
    for (int i = 0; i < count; i++) {
      boolean isNull = isNull(bitmapOffset, i);
      column.isNull[destinationIndex(indices, offset, i, repeating)] = isNull;
      if (isNull) {
        column.noNulls = false;
      }
    }
  }

  /** Null indices describe a consecutive range starting at offset; otherwise offset is unused. */
  private void readAppendColumn(ColumnVector column, int[] indices, int offset, int rowCount)
      throws IOException {
    assert !column.isRepeating;

    int flags = readUnsignedByte();
    boolean repeating = (flags & IS_REPEATING) != 0;
    // Effective top-level repetition is decoded before append; nested repetition is standalone.
    assert !repeating || rowCount <= 1;

    int count = repeating ? Math.min(rowCount, 1) : rowCount;
    int bitmapOffset = readNullBitmap(flags, count);
    if (!repeating && bitmapOffset < 0 && indices == null
        && VectorShuffleBatchFixedWidth.canRead(column, (flags & IS_DECIMAL_64) != 0)) {
      // A dense primitive frame appends consecutive rows. Avoid the generic per-value
      // index and null checks; prior frames may have made the accumulator nullable.
      if (!column.noNulls) {
        Arrays.fill(column.isNull, offset, offset + count, false);
      }
      readDenseFixedWidth(column, offset, count);
      return;
    }
    if (column instanceof StructColumnVector || column instanceof UnionColumnVector
        || column instanceof MultiValuedColumnVector) {
      // Complex columns need parent nulls before constructing their child mappings. Primitive
      // columns initialize null state in the same loop that decodes their values below.
      readNulls(column, indices, offset, count, false, bitmapOffset);
    }
    if (column instanceof StructColumnVector) {
      int activeCount = 0;
      for (int i = 0; i < count; i++) {
        if (!isNull(bitmapOffset, i)) {
          activeCount++;
        }
      }
      int[] active = new int[activeCount];
      int next = 0;
      for (int i = 0; i < count; i++) {
        if (!isNull(bitmapOffset, i)) {
          active[next++] = destinationIndex(indices, offset, i, false);
        }
      }
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        readAppendColumn(field, active, 0, activeCount);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < count; i++) {
        if (!isNull(bitmapOffset, i)) {
          int tag = readInt();
          union.tags[destinationIndex(indices, offset, i, false)] = tag;
          counts[tag]++;
        }
      }
      int[][] active = new int[union.fields.length][];
      int[] next = new int[union.fields.length];
      for (int tag = 0; tag < union.fields.length; tag++) {
        active[tag] = new int[counts[tag]];
      }
      for (int i = 0; i < count; i++) {
        if (!isNull(bitmapOffset, i)) {
          int index = destinationIndex(indices, offset, i, false);
          int tag = union.tags[index];
          active[tag][next[tag]++] = index;
        }
      }
      for (int tag = 0; tag < union.fields.length; tag++) {
        readAppendColumn(union.fields[tag], active[tag], 0, counts[tag]);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      MultiValuedColumnVector multi = (MultiValuedColumnVector) column;
      int oldChildCount = multi.childCount;
      int childCount = oldChildCount;
      for (int i = 0; i < count; i++) {
        int index = destinationIndex(indices, offset, i, false);
        multi.offsets[index] = childCount;
        int length = isNull(bitmapOffset, i) ? 0 : readInt();
        childCount += length;
        multi.lengths[index] = length;
      }
      int incomingChildren = childCount - oldChildCount;
      if (column instanceof ListColumnVector) {
        readAppendColumnConsecutive(((ListColumnVector) column).child, oldChildCount, incomingChildren);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        readAppendColumnConsecutive(map.keys, oldChildCount, incomingChildren);
        readAppendColumnConsecutive(map.values, oldChildCount, incomingChildren);
      }
      multi.childCount = childCount;
    } else {
      // The source repeating flag must never redirect writes to the accumulator's row zero.
      readValue(column, indices, offset, count, false, bitmapOffset, (flags & IS_DECIMAL_64) != 0);
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

    int bitmapOffset = readNullBitmap(flags, valueCount);

    column.isRepeating = repeating;
    column.noNulls = !hasNulls;
    if (!repeating && !hasNulls && destinationIndices == null
        && VectorShuffleBatchFixedWidth.canRead(column, (flags & IS_DECIMAL_64) != 0)) {
      // Standalone decoding must also overwrite any stale null bitmap entries.
      Arrays.fill(column.isNull, 0, valueCount, false);
      readDenseFixedWidth(column, 0, valueCount);
      return;
    }
    if (column instanceof StructColumnVector ||
        column instanceof UnionColumnVector ||
        column instanceof MultiValuedColumnVector) {
      readNulls(column, destinationIndices, 0, valueCount, repeating, bitmapOffset);
    }

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
      readValue(column, destinationIndices, 0, valueCount, repeating, bitmapOffset, sourceIsDecimal64);
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
      assert field.isNull.length >= struct.isNull.length;
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
      assert field.isNull.length >= union.tags.length;
      readColumn(field, fieldDestinationIndices[tag], fieldCounts[tag]);
    }
  }

  private int readMultiValuedLengths(
      MultiValuedColumnVector column, int[] destinationIndices, int valueCount, boolean repeating) {
    int childCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      int destinationIndex = destinationIndex(destinationIndices, logical, repeating);
      column.offsets[destinationIndex] = childCount;
      if (!column.isNull[destinationIndex]) {
        int length = readInt();
        childCount += length;
        column.lengths[destinationIndex] = length;
      } else {
        column.lengths[destinationIndex] = 0;
      }
    }
    column.childCount = childCount;
    return childCount;
  }

  private int readUnsignedByte() {
    return buffer[position++] & 0xff;
  }

  private int readInt() {
    int value = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Integer.BYTES;
    return value;
  }

  private long readLong() {
    long value = theUnsafe.getLong(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Long.BYTES;
    return value;
  }

  private double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  private boolean isNull(int bitmapOffset, int logical) {
    return bitmapOffset >= 0 && (buffer[bitmapOffset + (logical >>> 3)] & (1 << (logical & 7))) != 0;
  }

  private int destinationIndex(int[] destinationIndices, int logical, boolean repeating) {
    return destinationIndex(destinationIndices, 0, logical, repeating);
  }

  private int destinationIndex(int[] destinationIndices, int destinationOffset, int logical,
      boolean repeating) {
    return repeating ? 0 :
      (destinationIndices == null ? destinationOffset + logical : destinationIndices[logical]);
  }

  private int nonNullDestinationIndex(ColumnVector column, int[] destinationIndices,
      int destinationOffset, int logical, boolean repeating, int bitmapOffset) {
    int index = destinationIndex(destinationIndices, destinationOffset, logical, repeating);
    if (isNull(bitmapOffset, logical)) {
      column.isNull[index] = true;
      column.noNulls = false;
      return -1;
    }
    column.isNull[index] = false;
    return index;
  }

  private void readDenseFixedWidth(ColumnVector column, int destinationOffset, int count)
      throws IOException {
    long bytes = (long) count * Long.BYTES;
    // Unsafe.copyMemory has no Java array bounds checks. Validate the source slice first.
    if (count < 0 || bytes > limit - position) {
      throw new IOException("Truncated fixed-width vector shuffle column");
    }
    VectorShuffleBatchFixedWidth.read(buffer, position, column, destinationOffset, count);
    position += (int) bytes;
  }

  private void readValue(ColumnVector column, int[] destinationIndices, int destinationOffset,
      int valueCount, boolean repeating, int bitmapOffset, boolean sourceIsDecimal64)
      throws IOException {
    if (sourceIsDecimal64) {
      if (column instanceof Decimal64ColumnVector) {
        Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          int index = nonNullDestinationIndex(
              column, destinationIndices, destinationOffset, logical, repeating, bitmapOffset);
          if (index >= 0) {
            decimal64.vector[index] = readLong();
          }
        }
      } else {
        assert column instanceof DecimalColumnVector;
        DecimalColumnVector decimal = (DecimalColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
              repeating, bitmapOffset);
          if (index >= 0) {
            decimal.vector[index].deserialize64(readLong(), decimal.scale);
          }
        }
      }
      return;
    }
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytesColumn = (BytesColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          int length = readInt();
          bytesColumn.setVal(index, buffer, position, length);
          position += length;
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          timestamp.time[index] = readLong();
          timestamp.nanos[index] = readInt();
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      IntervalDayTimeColumnVector interval = (IntervalDayTimeColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          interval.set(index, new HiveIntervalDayTime(readLong(), readInt()));
        }
      }
    } else if (column instanceof Decimal64ColumnVector) {
      Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          HiveDecimalWritable decimal = new HiveDecimalWritable();
          position += decimal.readDirect(buffer, position);
          decimal64.set(index, decimal);
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          position += decimal.vector[index].readDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector is handled above; DateColumnVector shares this representation.
      LongColumnVector longs = (LongColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          longs.vector[index] = readLong();
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          doubles.vector[index] = readDouble();
        }
      }
    } else if (column instanceof VoidColumnVector) {
      // VOID has no value payload.
      for (int logical = 0; logical < valueCount; logical++) {
        nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical, repeating,
            bitmapOffset);
      }
    } else {
      throw unsupported(column);
    }
  }

  private IllegalArgumentException unsupported(ColumnVector column) {
    return new IllegalArgumentException(
        "Unsupported vector shuffle column type " + column.getClass().getName());
  }
}
