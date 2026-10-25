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

/** Deserializes a compact vector shuffle payload into an already schema-initialized batch. */
public final class VectorShuffleBatchDeserializer {
  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;
  private static final int IS_DECIMAL_64 = 4;

  private byte[] buffer;
  private int position;
  private int limit;
  private final FrameInfo frameInfo = new FrameInfo();
  private final boolean requiresMetadataInspection;
  // Fixed-schema accumulation always uses batches created by the same VectorizedRowBatchCtx.
  private final int fixedAppendCapacity;

  /** Keeps full metadata preinspection when the caller does not provide a fixed schema. */
  public VectorShuffleBatchDeserializer() {
    requiresMetadataInspection = true;
    fixedAppendCapacity = 0;
  }

  /**
   * Caches whether the transmitted input columns contain batch-wide date/timestamp metadata.
   * Scratch columns after {@code inputColumnCount} do not participate. The caller must use this fixed
   * schema, including its nested column types, for every destination passed to inspect().
   */
  public VectorShuffleBatchDeserializer(VectorizedRowBatch schema, int inputColumnCount) {
    assert schema != null;
    assert inputColumnCount >= 0 && inputColumnCount <= schema.cols.length;
    boolean hasMetadata = false;
    for (int i = 0; i < inputColumnCount; i++) {
      hasMetadata |= hasTypeMetadata(schema.cols[i]);
    }
    requiresMetadataInspection = hasMetadata;
    fixedAppendCapacity = calculateAppendCapacity(schema, inputColumnCount);
  }

  /** Row limit shared with the fixed-schema accumulation receiver. */
  public int getAppendCapacity() {
    assert fixedAppendCapacity > 0;
    return fixedAppendCapacity;
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

  /** Reusable inspection result, valid until the next inspect() call on this deserializer. */
  public static final class FrameInfo {
    private int rowCount;
    private boolean appendCompatible;

    private FrameInfo() {
    }

    public int getRowCount() {
      return rowCount;
    }

    /** Full frames, as well as incompatible metadata, require standalone replacement decoding. */
    public boolean isAppendCompatible() {
      return appendCompatible;
    }
  }

  /**
   * Validates the header and inspects appendable frames once, without changing the destination.
   * Full frames cannot share an execution batch, so only their header is inspected; replacement
   * decoding validates their payload. For a fixed schema without date/timestamp metadata, positive
   * smaller frames also need only header inspection; append decoding validates their payload.
   * Otherwise smaller frames are fully validated and their date/timestamp metadata is compared with
   * the destination, including when the destination is empty. Empty frames are fully validated.
   *
   * The result is reused and holds no input-buffer or destination references. Callers must consume
   * its fields before the next inspection, and flush then use replacement decoding if it is not
   * append-compatible.
   */
  public FrameInfo inspect(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    try {
      start(serialized);
      int rowCount = readInt();
      int columnCount = readInt();
      int capacity = validateAppendDimensions(destination, rowCount, columnCount);
      boolean compatible = rowCount < capacity;
      if (rowCount == 0 || (compatible && requiresMetadataInspection)) {
        for (int i = 0; i < columnCount; i++) {
          compatible &= inspectColumn(destination.cols[i], rowCount);
        }
        checkEnd();
      }
      frameInfo.rowCount = rowCount;
      frameInfo.appendCompatible = compatible;
      return frameInfo;
    } finally {
      buffer = null;
    }
  }

  /**
   * Appends directly into destination-owned storage, preserving its dense, nonrepeating prefix.
   * Only successful decoding publishes the new size. A failure invalidates the destination; callers
   * must fail the task and discard it, rather than dispatching the partially decoded batch.
   * This standalone API checks the frame header and capacity itself. The accumulation receiver
   * uses appendInspected() to reuse the header already validated by inspect().
   */
  public void append(BytesWritable serialized, VectorizedRowBatch destination) throws IOException {
    assert destination != null && !destination.selectedInUse;
    try {
      start(serialized);
      int rowCount = readInt();
      int columnCount = readInt();
      int capacity = validateAppendDimensions(destination, rowCount, columnCount);
      validateAppendCapacity(destination, rowCount, capacity);
      appendColumns(destination, columnCount, rowCount);
    } finally {
      buffer = null;
    }
  }

  /**
   * Append a frame whose header has already been validated by inspect(). The caller supplies
   * a snapshot of its row count because downstream processing may run between inspection and
   * append. Reposition at the start of the columns without rereading either header integer or
   * recomputing the destination's capacity. Payload validation, including checkEnd(), remains.
   */
  public void appendInspected(BytesWritable serialized, VectorizedRowBatch destination, int rowCount)
      throws IOException {
    assert destination != null && !destination.selectedInUse && fixedAppendCapacity > 0;
    try {
      start(serialized);
      position += 2 * Integer.BYTES;
      // The receiver may have flushed and replaced the destination since inspect().
      validateAppendCapacity(destination, rowCount, fixedAppendCapacity);
      appendColumns(destination, destination.projectionSize, rowCount);
    } finally {
      buffer = null;
    }
  }

  private void appendColumns(VectorizedRowBatch destination, int columnCount, int rowCount)
      throws IOException {
    for (int i = 0; i < columnCount; i++) {
      // Header/capacity validation already guarantees space in every top-level column.
      readAppendColumn(destination.cols[i], null, destination.size, rowCount);
    }
    checkEnd();
    destination.size += rowCount;
  }

  private void start(BytesWritable serialized) throws IOException {
    assert serialized != null;
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
    assert destination != null;
    if (rowCount < 0 || columnCount < 0
        || columnCount > destination.cols.length) {
      throw new IOException("Invalid vector shuffle batch dimensions: " + rowCount + " rows, "
          + columnCount + " columns");
    }
  }

  private int validateAppendDimensions(VectorizedRowBatch destination, int rowCount,
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
    return capacity;
  }

  private void validateAppendCapacity(VectorizedRowBatch destination, int rowCount, int capacity)
      throws IOException {
    assert destination.size >= 0;
    if (destination.size > capacity - rowCount) {
      throw new IOException("Vector shuffle frame of " + rowCount + " rows does not fit destination"
          + " with " + destination.size + " pending rows and capacity " + capacity);
    }
  }

  private int appendCapacity(VectorizedRowBatch destination) {
    return fixedAppendCapacity > 0
        ? fixedAppendCapacity : calculateAppendCapacity(destination, destination.projectionSize);
  }

  private static int calculateAppendCapacity(VectorizedRowBatch batch, int columnCount) {
    int capacity = Math.min(VectorizedRowBatch.DEFAULT_SIZE, batch.selected.length);
    for (int i = 0; i < columnCount; i++) {
      assert batch.cols[i] != null;
      capacity = Math.min(capacity, batch.cols[i].isNull.length);
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
      checkEnd();
    } finally {
      buffer = null;
    }
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

  private int readNullBitmap(int flags, int valueCount) throws IOException {
    if ((flags & HAS_NULLS) == 0) {
      return -1;
    }
    int length = (int) ((valueCount + 7L) / 8);
    int bitmapOffset = position;
    skipBytes(length);
    return bitmapOffset;
  }

  private boolean inspectColumn(ColumnVector column, int rowCount) throws IOException {
    int flags = readFlags(column);
    int count = (flags & IS_REPEATING) != 0 ? Math.min(rowCount, 1) : rowCount;
    boolean hasNulls = (flags & HAS_NULLS) != 0;
    int bitmapOffset = position;
    if (hasNulls) {
      // Validate the entire bitmap before inspecting its bits directly in the input buffer.
      skipBytes((int) ((count + 7L) / 8));
    }
    boolean compatible = inspectTypeMetadata(column, count);
    int nonNullCount = count;
    if (hasNulls) {
      for (int i = 0; i < count; i++) {
        if ((buffer[bitmapOffset + (i >>> 3)] & (1 << (i & 7))) != 0) {
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
        position += decimalLength();
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

  private void checkFixedValues(int count, int width) throws IOException {
    assert count >= 0 && width > 0;
    if (count > (limit - position) / width) {
      throw new IOException("Truncated vector shuffle values");
    }
  }

  private void skipFixedValues(int count, int width) throws IOException {
    checkFixedValues(count, width);
    position += count * width;
  }

  // Count encoded (non-null) values once per fixed-width column, not once per row.
  private void checkFixedColumn(int bitmapOffset, int valueCount, int width) throws IOException {
    int nonNullCount = valueCount;
    if (bitmapOffset >= 0) {
      int fullBytes = valueCount >>> 3;
      for (int i = 0; i < fullBytes; i++) {
        nonNullCount -= Integer.bitCount(buffer[bitmapOffset + i] & 0xff);
      }
      int remaining = valueCount & 7;
      if (remaining != 0) {
        int mask = (1 << remaining) - 1;
        nonNullCount -= Integer.bitCount(buffer[bitmapOffset + fullBytes] & mask);
      }
    }
    checkFixedValues(nonNullCount, width);
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

  // Appended rows occupy previously unwritten slots; resetInit() clears old null flags.
  private void readNulls(ColumnVector column, int[] indices, int offset, int count,
      boolean repeating, int bitmapOffset) {
    for (int i = 0; i < count; i++) {
      if (isNull(bitmapOffset, i)) {
        column.isNull[destinationIndex(indices, offset, i, repeating)] = true;
        column.noNulls = false;
      }
    }
  }

  /** Null indices describe a consecutive range starting at offset; otherwise offset is unused. */
  private void readAppendColumn(ColumnVector column, int[] indices, int offset, int rowCount)
      throws IOException {
    assert !column.isRepeating;
    int flags = readFlags(column);
    boolean repeating = (flags & IS_REPEATING) != 0;
    int count = repeating ? Math.min(rowCount, 1) : rowCount;
    int bitmapOffset = readNullBitmap(flags, count);
    if (column instanceof StructColumnVector || column instanceof UnionColumnVector
        || column instanceof MultiValuedColumnVector) {
      // Complex columns need parent nulls before constructing their child mappings. Primitive
      // columns initialize null state in the same loop that decodes their values below.
      readNulls(column, indices, offset, count, false, bitmapOffset);
    }
    // Recheck metadata during decoding, including after a receiver flush, unless the fixed schema
    // has none. Empty nested columns leave destination metadata untouched.
    if (requiresMetadataInspection && !inspectTypeMetadata(column, count)) {
      throw new IOException("Incompatible vector shuffle date/timestamp metadata");
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
          validateUnionTag(tag, union.fields.length);
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
        childCount = checkedAdd(childCount, length);
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
      expandRepeating(column, indices, offset, rowCount);
    }
  }

  private void expandRepeating(ColumnVector column, int[] indices, int offset, int rowCount) {
    int source = destinationIndex(indices, offset, 0, false);
    if (indices == null) {
      int end = offset + rowCount;
      if (column.isNull[source]) {
        // Use the post-decode null state, including a decimal conversion that produced null.
        Arrays.fill(column.isNull, offset + 1, end, true);
        column.noNulls = false;
        return;
      }
      if (column instanceof LongColumnVector) {
        LongColumnVector longs = (LongColumnVector) column;
        Arrays.fill(longs.vector, offset + 1, end, longs.vector[source]);
        return;
      }
      if (column instanceof DoubleColumnVector) {
        DoubleColumnVector doubles = (DoubleColumnVector) column;
        Arrays.fill(doubles.vector, offset + 1, end, doubles.vector[source]);
        return;
      }
    }
    for (int i = 1; i < rowCount; i++) {
      copyRepeatedElement(column, source, destinationIndex(indices, offset, i, false));
    }
  }

  private void copyRepeatedElement(ColumnVector column, int source, int destination) {
    if (column.isNull[source]) {
      column.isNull[destination] = true;
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
    final int flags = readFlags(column);
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
        childCount = checkedAdd(childCount, length);
        column.lengths[destinationIndex] = length;
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

  // Only use these after checkFixedColumn() has validated the full encoded column.
  private int readFixedInt() {
    int value = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Integer.BYTES;
    return value;
  }

  private long readFixedLong() {
    long value = theUnsafe.getLong(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    position += Long.BYTES;
    return value;
  }

  private double readFixedDouble() {
    return Double.longBitsToDouble(readFixedLong());
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

  // New destination slots are already non-null after initialization or batch reset.
  private int nonNullDestinationIndex(ColumnVector column, int[] destinationIndices,
      int destinationOffset, int logical, boolean repeating, int bitmapOffset) {
    int index = destinationIndex(destinationIndices, destinationOffset, logical, repeating);
    if (isNull(bitmapOffset, logical)) {
      column.isNull[index] = true;
      column.noNulls = false;
      return -1;
    }
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
      checkFixedColumn(bitmapOffset, valueCount, Long.BYTES);
      if (column instanceof Decimal64ColumnVector) {
        Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
              repeating, bitmapOffset);
          if (index >= 0) {
            decimal64.vector[index] = readFixedLong();
          }
        }
      } else {
        assert column instanceof DecimalColumnVector;
        DecimalColumnVector decimal = (DecimalColumnVector) column;
        for (int logical = 0; logical < valueCount; logical++) {
          int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
              repeating, bitmapOffset);
          if (index >= 0) {
            decimal.vector[index].deserialize64(readFixedLong(), decimal.scale);
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
          requireBytes(length);
          bytesColumn.setVal(index, buffer, position, length);
          position += length;
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      checkFixedColumn(bitmapOffset, valueCount, Long.BYTES + Integer.BYTES);
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          timestamp.time[index] = readFixedLong();
          timestamp.nanos[index] = readFixedInt();
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      checkFixedColumn(bitmapOffset, valueCount, Long.BYTES + Integer.BYTES);
      IntervalDayTimeColumnVector interval = (IntervalDayTimeColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          interval.set(index, new HiveIntervalDayTime(readFixedLong(), readFixedInt()));
        }
      }
    } else if (column instanceof Decimal64ColumnVector) {
      Decimal64ColumnVector decimal64 = (Decimal64ColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          HiveDecimalWritable decimal = new HiveDecimalWritable();
          decimalLength();
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
          decimalLength();
          position += decimal.vector[index].readDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector is handled above; DateColumnVector shares this representation.
      checkFixedColumn(bitmapOffset, valueCount, Long.BYTES);
      LongColumnVector longs = (LongColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          longs.vector[index] = readFixedLong();
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      checkFixedColumn(bitmapOffset, valueCount, Long.BYTES);
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        int index = nonNullDestinationIndex(column, destinationIndices, destinationOffset, logical,
            repeating, bitmapOffset);
        if (index >= 0) {
          doubles.vector[index] = readFixedDouble();
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
