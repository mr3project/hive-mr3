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
  private final boolean accumulateFrames;
  private final FrameInfo frameInfo;
  private final boolean[] accumulatorRepeating;
  private final byte[][] repeatedColumnEncodings;
  private boolean hasRepeatingColumns;

  /** Reused inspection result; valid until the next inspectAccumulation() call. */
  public static final class FrameInfo {
    private final int[] columnStarts;
    private final int[] columnEnds;
    private final boolean[] columnRepeating;
    private boolean appendCompatible;
    private boolean repeatingCompatible;

    private FrameInfo(int columnCount) {
      columnStarts = new int[columnCount];
      columnEnds = new int[columnCount];
      columnRepeating = new boolean[columnCount];
    }

    public boolean isAppendCompatible() {
      return appendCompatible;
    }

    public boolean isRepeatingCompatible() {
      return repeatingCompatible;
    }
  }

  public VectorShuffleBatchAccumulatingDeserializer() {
    fixedAppendCapacity = 0;
    accumulateFrames = false;
    frameInfo = null;
    accumulatorRepeating = null;
    repeatedColumnEncodings = null;
  }

  /**
   * Caches the receiver's capacity and metadata policy for its fixed input schema.
   * Calendar and UTC flags describe an entire vector, so schemas containing those flags use
   * standalone replacement decoding instead of mixing frames with possibly different metadata.
   * Scratch columns after {@code inputColumnCount} do not participate.
   */
  public VectorShuffleBatchAccumulatingDeserializer(VectorizedRowBatch schema, int inputColumnCount) {
    assert schema != null;
    assert inputColumnCount >= 0 && inputColumnCount <= schema.cols.length;
    boolean hasMetadata = false;
    for (int i = 0; i < inputColumnCount; i++) {
      hasMetadata |= hasTypeMetadata(schema.cols[i]);
    }
    accumulateFrames = !hasMetadata;
    fixedAppendCapacity = calculateAppendCapacity(schema, inputColumnCount);
    frameInfo = new FrameInfo(inputColumnCount);
    accumulatorRepeating = new boolean[inputColumnCount];
    repeatedColumnEncodings = new byte[inputColumnCount][];
  }

  /** Row limit shared with the fixed-schema accumulation receiver. */
  public int getAppendCapacity() {
    assert fixedAppendCapacity > 0;
    return fixedAppendCapacity;
  }

  public boolean canAccumulateFrames() {
    return accumulateFrames;
  }

  public boolean hasRepeatingColumns() {
    return hasRepeatingColumns;
  }

  /** Forget repeating encodings whenever the receiver dispatches or discards its batch. */
  public void resetAccumulation() {
    assert accumulatorRepeating != null;
    Arrays.fill(accumulatorRepeating, false);
    Arrays.fill(repeatedColumnEncodings, null);
    hasRepeatingColumns = false;
  }

  /** Reads the row count needed by the receiver's batching decision without walking the payload. */
  public static int getRowCount(BytesWritable serialized) {
    return theUnsafe.getInt(serialized.getBytesRaw(), BYTE_ARRAY_BASE_OFFSET + serialized.getOffset());
  }

  private static boolean hasTypeMetadata(ColumnVector column) {
    assert column != null;
    // DateColumnVector extends LongColumnVector, but still carries batch-wide metadata.
    if (column instanceof DateColumnVector || column instanceof TimestampColumnVector) {
      return true;
    }
    if (column instanceof ListColumnVector) {
      return hasTypeMetadata(((ListColumnVector) column).child);
    }
    if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      boolean hasMetadata = hasTypeMetadata(map.keys);
      hasMetadata |= hasTypeMetadata(map.values);
      return hasMetadata;
    }
    ColumnVector[] fields;
    if (column instanceof StructColumnVector) {
      fields = ((StructColumnVector) column).fields;
    } else if (column instanceof UnionColumnVector) {
      fields = ((UnionColumnVector) column).fields;
    } else {
      return false;
    }
    boolean hasMetadata = false;
    for (ColumnVector field : fields) {
      hasMetadata |= hasTypeMetadata(field);
    }
    return hasMetadata;
  }

  /**
   * Inspect column boundaries and repeating flags without decoding any values.
   * Top-level repeating columns retain their row-zero value across compatible frames.
   * Nested repeating vectors are processed standalone rather than expanded during append.
   */
  public FrameInfo inspectAccumulation(BytesWritable serialized, VectorizedRowBatch destination,
      int rowCount) throws IOException {
    assert accumulateFrames && frameInfo != null;
    try {
      start(serialized);
      position += 2 * Integer.BYTES;
      boolean nestedRepeating = false;
      for (int i = 0; i < destination.projectionSize; i++) {
        frameInfo.columnStarts[i] = position - serialized.getOffset();
        frameInfo.columnRepeating[i] =
            rowCount > 1 && (buffer[position] & IS_REPEATING) != 0;
        nestedRepeating |= skipColumn(destination.cols[i], rowCount, false);
        frameInfo.columnEnds[i] = position - serialized.getOffset();
      }
      checkFrameEnd();
      boolean compatible = true;
      if (destination.size > 0) {
        for (int i = 0; i < destination.projectionSize; i++) {
          if (accumulatorRepeating[i]) {
            if ((rowCount > 1 && !frameInfo.columnRepeating[i])
                || !matchesRepeatingColumn(i, serialized.getOffset())) {
              compatible = false;
            }
          } else if (frameInfo.columnRepeating[i]) {
            compatible = false;
          }
        }
      }
      frameInfo.appendCompatible = !nestedRepeating;
      frameInfo.repeatingCompatible = compatible;
      return frameInfo;
    } finally {
      buffer = null;
    }
  }

  private boolean matchesRepeatingColumn(int column, int frameOffset) {
    byte[] previous = repeatedColumnEncodings[column];
    int start = frameInfo.columnStarts[column];
    int end = frameInfo.columnEnds[column];
    if (previous == null || previous.length != end - start) {
      return false;
    }
    int source = frameOffset + start;
    // A one-row frame may encode the same value without IS_REPEATING.
    return (previous[0] & ~IS_REPEATING) == (buffer[source] & ~IS_REPEATING)
        && Arrays.equals(previous, 1, previous.length, buffer, source + 1,
            source + previous.length);
  }

  /**
   * Skip a trusted column's wire encoding. No bounds or content validation is performed.
   * Return true if any nested vector would require expansion during accumulation.
   */
  private boolean skipColumn(ColumnVector column, int rowCount, boolean nested) {
    int flags = readUnsignedByte();
    boolean repeating = (flags & IS_REPEATING) != 0;
    int count = repeating ? Math.min(rowCount, 1) : rowCount;
    boolean nestedRepeating = nested && repeating && rowCount > 1;
    int bitmapOffset = readNullBitmap(flags, count);
    int nonNullCount = count;
    if (bitmapOffset >= 0) {
      for (int i = 0; i < count; i++) {
        if (isNull(bitmapOffset, i)) {
          nonNullCount--;
        }
      }
    }

    // Accumulation is disabled for schemas containing date/timestamp metadata.
    assert !(column instanceof DateColumnVector) && !(column instanceof TimestampColumnVector);
    if (column instanceof StructColumnVector) {
      for (ColumnVector field : ((StructColumnVector) column).fields) {
        nestedRepeating |= skipColumn(field, nonNullCount, true);
      }
    } else if (column instanceof UnionColumnVector) {
      UnionColumnVector union = (UnionColumnVector) column;
      int[] counts = new int[union.fields.length];
      for (int i = 0; i < nonNullCount; i++) {
        counts[readInt()]++;
      }
      for (int i = 0; i < counts.length; i++) {
        nestedRepeating |= skipColumn(union.fields[i], counts[i], true);
      }
    } else if (column instanceof MultiValuedColumnVector) {
      int children = 0;
      for (int i = 0; i < nonNullCount; i++) {
        children += readInt();
      }
      if (column instanceof ListColumnVector) {
        nestedRepeating |= skipColumn(((ListColumnVector) column).child, children, true);
      } else {
        MapColumnVector map = (MapColumnVector) column;
        nestedRepeating |= skipColumn(map.keys, children, true);
        nestedRepeating |= skipColumn(map.values, children, true);
      }
    } else if ((flags & IS_DECIMAL_64) != 0
        || (column instanceof LongColumnVector && !(column instanceof Decimal64ColumnVector))
        || column instanceof DoubleColumnVector) {
      position += nonNullCount * Long.BYTES;
    } else if (column instanceof BytesColumnVector) {
      for (int i = 0; i < nonNullCount; i++) {
        int length = readInt();
        position += length;
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      position += nonNullCount * (Long.BYTES + Integer.BYTES);
    } else if (column instanceof DecimalColumnVector || column instanceof Decimal64ColumnVector) {
      for (int i = 0; i < nonNullCount; i++) {
        // Direct decimal encoding stores a big-endian byte-array length after its scale.
        int lengthOffset = position + Integer.BYTES;
        int length = ((buffer[lengthOffset] & 0xff) << 24)
            | ((buffer[lengthOffset + 1] & 0xff) << 16)
            | ((buffer[lengthOffset + 2] & 0xff) << 8)
            | (buffer[lengthOffset + 3] & 0xff);
        position += 2 * Integer.BYTES + length;
      }
    } else if (!(column instanceof VoidColumnVector)) {
      throw unsupported(column);
    }
    return nestedRepeating;
  }

  /**
   * Retain the optimized unchecked value decoder but preserve repeating top-level columns.
   * The caller must inspect the frame and flush any incompatible accumulator first.
   */
  public void appendInspectedAccumulation(BytesWritable serialized, VectorizedRowBatch destination,
      int rowCount) throws IOException {
    assert frameInfo != null && frameInfo.appendCompatible && !destination.selectedInUse;
    assert destination.size == 0 || frameInfo.repeatingCompatible;
    try {
      start(serialized);
      position += 2 * Integer.BYTES;
      int originalRows = destination.size;
      for (int i = 0; i < destination.projectionSize; i++) {
        assert position == serialized.getOffset() + frameInfo.columnStarts[i];
        if (originalRows > 0 && accumulatorRepeating[i]) {
          position = serialized.getOffset() + frameInfo.columnEnds[i];
        } else {
          readAppendColumn(destination.cols[i], null, originalRows, rowCount, true);
        }
      }
      checkFrameEnd();
      destination.size += rowCount;
      if (originalRows == 0) {
        for (int i = 0; i < destination.projectionSize; i++) {
          boolean repeating = frameInfo.columnRepeating[i];
          accumulatorRepeating[i] = repeating;
          if (repeating) {
            int offset = serialized.getOffset();
            repeatedColumnEncodings[i] = Arrays.copyOfRange(buffer,
                offset + frameInfo.columnStarts[i], offset + frameInfo.columnEnds[i]);
            hasRepeatingColumns = true;
          }
        }
      }
    } finally {
      buffer = null;
    }
  }

  /** Convenience API for callers that do not already have an inspection result. */
  public void append(BytesWritable serialized, VectorizedRowBatch destination, int rowCount)
      throws IOException {
    FrameInfo info = inspectAccumulation(serialized, destination, rowCount);
    if (!info.isAppendCompatible() || (destination.size > 0 && !info.isRepeatingCompatible())) {
      throw new IOException("Incompatible vector shuffle accumulation frame");
    }
    appendInspectedAccumulation(serialized, destination, rowCount);
  }

  private void start(BytesWritable serialized) {
    assert serialized != null;
    buffer = serialized.getBytesRaw();
    position = serialized.getOffset();
    limit = position + serialized.getLength();
  }

  private void checkFrameEnd() throws IOException {
    if (position != limit) {
      throw new IOException("Vector shuffle batch has " + (limit - position) + " trailing bytes");
    }
  }

  private static int calculateAppendCapacity(VectorizedRowBatch batch, int columnCount) {
    int capacity = Math.min(VectorizedRowBatch.DEFAULT_SIZE, batch.selected.length);
    for (int i = 0; i < columnCount; i++) {
      assert batch.cols[i] != null;
      capacity = Math.min(capacity, batch.cols[i].isNull.length);
    }
    return capacity;
  }

  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    assert serialized != null && destination != null;
    try {
      start(serialized);
      final int rowCount = readInt();
      final int columnCount = readInt();

      destination.reset();
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        assert destination.cols[columnIndex] != null;
        destination.cols[columnIndex].ensureSize(rowCount, false);
      }
      destination.size = rowCount;
      destination.selectedInUse = false;
      destination.projectionSize = columnCount;
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        destination.projectedColumns[columnIndex] = columnIndex;
        ColumnVector column = destination.cols[columnIndex];
        readColumn(column, rowCount);
      }
      checkFrameEnd();
    } finally {
      buffer = null;
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

  private void readAppendColumn(ColumnVector column, int[] indices, int rowCount)
      throws IOException {
    column.ensureSize(requiredSize(indices, rowCount, false), true);
    readAppendColumn(column, indices, 0, rowCount);
  }

  private void readAppendColumnConsecutive(ColumnVector column, int offset, int rowCount)
      throws IOException {
    assert offset >= 0 && rowCount >= 0 && offset <= Integer.MAX_VALUE - rowCount;
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
    readAppendColumn(column, indices, offset, rowCount, false);
  }

  private void readAppendColumn(ColumnVector column, int[] indices, int offset, int rowCount,
      boolean preserveRepeating) throws IOException {
    assert !column.isRepeating;
    int flags = readUnsignedByte();
    boolean repeating = (flags & IS_REPEATING) != 0;
    int count = repeating ? Math.min(rowCount, 1) : rowCount;
    int bitmapOffset = readNullBitmap(flags, count);
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
        readAppendColumn(field, active, activeCount);
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
        readAppendColumn(union.fields[tag], active[tag], counts[tag]);
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
        readAppendColumnConsecutive(((ListColumnVector) column).child, oldChildCount,
            incomingChildren);
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
    if (repeating && rowCount > 1) {
      // Nested repeating input is handled by standalone decoding, never expanded here.
      assert preserveRepeating && indices == null && offset == 0;
      column.isRepeating = true;
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

    int bitmapOffset = readNullBitmap(flags, valueCount);

    column.isRepeating = repeating;
    column.noNulls = !hasNulls;
    if (column instanceof StructColumnVector || column instanceof UnionColumnVector
        || column instanceof MultiValuedColumnVector) {
      readNulls(column, destinationIndices, 0, valueCount, repeating, bitmapOffset);
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

  private boolean readBoolean() {
    return readUnsignedByte() != 0;
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
    return bitmapOffset >= 0
        && (buffer[bitmapOffset + (logical >>> 3)] & (1 << (logical & 7))) != 0;
  }

  private int destinationIndex(int[] destinationIndices, int logical, boolean repeating) {
    return destinationIndex(destinationIndices, 0, logical, repeating);
  }

  private int destinationIndex(int[] destinationIndices, int destinationOffset, int logical,
      boolean repeating) {
    return repeating ? 0
        : destinationIndices == null ? destinationOffset + logical : destinationIndices[logical];
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

  private void readValue(ColumnVector column, int[] destinationIndices, int destinationOffset,
      int valueCount, boolean repeating, int bitmapOffset, boolean sourceIsDecimal64)
      throws IOException {
    if (sourceIsDecimal64) {
      if (column instanceof Decimal64ColumnVector) {
        Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
              repeating, bitmapOffset);
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
