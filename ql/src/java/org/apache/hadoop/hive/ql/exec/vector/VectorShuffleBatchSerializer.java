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

import static org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkCommonOperator.NUM_ROWS_THRESHOLD_FOR_BATCH;
import static org.apache.tez.util.FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.tez.util.FastByteComparisons.theUnsafe;

import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;

/**
 * Serializes the active rows of selected columns from a {@link VectorizedRowBatch}.
 *
 * <p>The receiver is expected to know the column schema. Selected rows
 * are compacted in logical order, inactive rows and unselected columns are omitted, null values are
 * omitted, and repeating columns are represented by a single value. Primitive, list, map, struct, and
 * union vectors are supported recursively.</p>
 *
 * <p>Call {@link #initialize(ColumnVector[], int[])} before serializing to set the schema and
 * column mapping and reuse schema metadata and scratch arrays. Call it again when the schema
 * or mapping changes.</p>
 *
 * <p>Frames contain the common prefix described by {@link VectorShuffleBatchHeader}, followed by
 * the partition row count, nested-repetition flag, and ordinary column payloads. Prepare the prefix
 * once with {@link #setupCommonPrefix(VectorizedRowBatch, int[], byte[], int)} to reuse it across
 * multiple compact partitions of the same input batch.</p>
 */
public final class VectorShuffleBatchSerializer {
  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;
  private static final int IS_DECIMAL_64 = 4;

  private byte[] buffer;
  private int position;
  // Accumulates nested repetition while serializing the current frame.
  private boolean nestedRepeating;
  private VectorShuffleBatchMetadata metadata;
  private int columnCount;
  private int bitmapLength;
  private int commonHeaderSize;
  private int[] repeatingColumns;
  private int[] ordinaryColumns;
  private CommonPrefix commonPrefix;

  /**
   * Serializer-owned preparation storage, valid until the next preparation or schema change.
   * The source input columns must remain unchanged while this prefix is being used. Neither the
   * preparation buffer nor the source vectors are retained here.
   */
  public static final class CommonPrefix {

    private byte[] bytes = new byte[0];
    private int length;
    private int[] ordinaryColumns = new int[0];
    private int ordinaryCount;
    // Preserve the prefix's flag before partition-specific columns are serialized.
    private boolean nestedRepeating;

    private CommonPrefix() {
    }

    private void setup(byte[] source, int offset, int length, int[] columns, int count, boolean nestedRepeating) {
      if (bytes.length < length) {
        bytes = new byte[length];
      }
      System.arraycopy(source, offset, bytes, 0, length);

      if (ordinaryColumns.length < count) {
        ordinaryColumns = new int[count];
      }
      System.arraycopy(columns, 0, ordinaryColumns, 0, count);

      this.length = length;
      this.ordinaryCount = count;
      this.nestedRepeating = nestedRepeating;
    }
  }

  /**
   * Precomputes the fixed schema and logical column mapping. Call again before serializing a
   * different schema or mapping. Paths refer to the current batch, so replacing its vectors with
   * vectors of the same schema does not require reinitialization.
   */
  public void initialize(ColumnVector[] columns, int[] sourceColumnMap) {
    metadata = new VectorShuffleBatchMetadata(columns, sourceColumnMap);
    columnCount = sourceColumnMap.length;
    bitmapLength = (columnCount + 7) / 8;
    commonHeaderSize = Integer.BYTES + bitmapLength + metadata.getByteCount();
    if (repeatingColumns == null || repeatingColumns.length != columnCount) {
      repeatingColumns = new int[columnCount];
      ordinaryColumns = new int[columnCount];
    }
  }

  public int serialize(VectorizedRowBatch source, int[] sourceColumnMap, byte[] buffer,
      int position) {
    return serializeColumns(source, sourceColumnMap,
        source.selectedInUse ? source.selected : null, 0, source.size, buffer, position);
  }

  public int serialize(VectorizedRowBatch source, int[] sourceColumnMap, int[] rowIndices,
      int rowOffset, int rowCount, byte[] buffer, int position) {
    assert rowOffset <= rowIndices.length - rowCount;
    return serializeColumns(source, sourceColumnMap, rowIndices, rowOffset, rowCount,
        buffer, position);
  }

  /**
   * Prepare the common bytes for multi-row partitions of one input batch. The supplied bounded
   * output buffer is scratch for encoding; successful preparation copies the prefix into owned,
   * reusable storage. A prefix that does not fit throws ArrayIndexOutOfBoundsException, just like
   * frame serialization, allowing the caller to fall back to row input without unbounded growth.
   * Call once, lazily, after obtaining the first eligible partition's output buffer.
   * Requires prior {@link #initialize(ColumnVector[], int[])}.
   */
  public void setupCommonPrefix(VectorizedRowBatch source, int[] sourceColumnMap,
      byte[] buffer, int position) {
    assert source.size > NUM_ROWS_THRESHOLD_FOR_BATCH;
    reset(buffer, position);
    int ordinaryCount = writeCommonPrefix(source, sourceColumnMap, null, 0, source.size);
    if (commonPrefix == null) {
      commonPrefix = new CommonPrefix();
    }
    commonPrefix.setup(buffer, position, this.position - position,
        ordinaryColumns, ordinaryCount, nestedRepeating);
  }

  /** Append one multi-row partition using the prefix set up for this input batch. */
  public int serializePartition(VectorizedRowBatch source, int[] rowIndices,
      int rowOffset, int rowCount, byte[] buffer, int position) {
    assert commonPrefix != null;
    assert rowCount > NUM_ROWS_THRESHOLD_FOR_BATCH;
    assert rowOffset >= 0 && rowCount <= rowIndices.length - rowOffset;

    reset(buffer, position);
    ensureCapacity((long) commonPrefix.length + Integer.BYTES + Byte.BYTES);  // once per partition prefix and header
    System.arraycopy(commonPrefix.bytes, 0, buffer, position, commonPrefix.length);

    this.position += commonPrefix.length;
    nestedRepeating = commonPrefix.nestedRepeating;
    writePartition(source, commonPrefix.ordinaryColumns, commonPrefix.ordinaryCount,
        rowIndices, rowOffset, rowCount);

    return this.position - position;
  }

  private int serializeColumns(VectorizedRowBatch source, int[] sourceColumnMap, int[] rowIndices,
      int rowOffset, int rowCount, byte[] buffer, int position) {
    reset(buffer, position);
    int ordinaryCount = writeCommonPrefix(source, sourceColumnMap, rowIndices, rowOffset, rowCount);
    ensureCapacity(Integer.BYTES + Byte.BYTES);   // once per partition header
    writePartition(source, ordinaryColumns, ordinaryCount, rowIndices, rowOffset, rowCount);
    return this.position - position;
  }

  private int writeCommonPrefix(VectorizedRowBatch source, int[] sourceColumnMap, int[] rowIndices,
      int rowOffset, int rowCount) {
    // add Integer.BYTES for writeInt() when repeatingCount != 0
    ensureCapacity(commonHeaderSize + Integer.BYTES);   // once per common prefix
    theUnsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET + position, columnCount);

    // Bits refer to the sourceColumnMap's logical order, least significant bit first.
    final int bitmapOffset = position + Integer.BYTES;
    final int metadataOffset = bitmapOffset + bitmapLength;
    position += commonHeaderSize;
    Arrays.fill(buffer, bitmapOffset, metadataOffset, (byte) 0);
    int repeatingCount = 0;
    int ordinaryCount = 0;
    for (int logical = 0; logical < columnCount; logical++) {
      int sourceColumn = sourceColumnMap[logical];
      if (rowCount > 1 && source.cols[sourceColumn].isRepeating) {
        buffer[bitmapOffset + (logical >>> 3)] |= 1 << (logical & 7);
        repeatingColumns[repeatingCount++] = sourceColumn;
      } else {
        ordinaryColumns[ordinaryCount++] = sourceColumn;
      }
    }
    if (metadata.getByteCount() != 0) {
      metadata.write(source.cols, buffer, metadataOffset);
    }

    final int repeatingLengthOffset = this.position;
    if (repeatingCount != 0) {
      writeInt(0);
    }
    final int repeatingStart = this.position;
    nestedRepeating = false;
    for (int i = 0; i < repeatingCount; i++) {
      writeColumn(source.cols[repeatingColumns[i]], rowIndices, rowOffset, rowCount, false);
    }
    if (repeatingCount != 0) {
      theUnsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET + repeatingLengthOffset,
          this.position - repeatingStart);
    }

    return ordinaryCount;
  }

  private void writePartition(VectorizedRowBatch source, int[] ordinaryColumns, int ordinaryCount,
      int[] rowIndices, int rowOffset, int rowCount) {
    writeInt(rowCount);

    final int nestedRepeatingOffset = position;
    // 0 = placeholder for the nestedRepeating flag
    writeByte(0);
    for (int i = 0; i < ordinaryCount; i++) {
      writeColumn(source.cols[ordinaryColumns[i]], rowIndices, rowOffset, rowCount, false);
    }
    // updated after all ordinary columns have been serialized
    buffer[nestedRepeatingOffset] = (byte) (nestedRepeating ? 1 : 0);
  }

  private void writeColumn(ColumnVector column, int[] indices, int rowOffset, int count,
      boolean nested) {
    final boolean repeating = column.isRepeating;
    // The count belongs to this invocation: an empty/singleton child never needs expansion.
    nestedRepeating |= nested && repeating && count > 1;

    final int valueCount = repeating ? Math.min(count, 1) : count;
    int firstNull = valueCount;
    if (!column.noNulls) {
      firstNull = 0;
      while (firstNull < valueCount &&
             !column.isNull[physicalIndex(indices, rowOffset, firstNull, repeating)]) {
        firstNull++;
      }
    }

    final boolean hasNulls = firstNull < valueCount;
    final int nullBitmapLength = hasNulls ? (valueCount + 7) / 8 : 0;
    ensureCapacity(Byte.BYTES + nullBitmapLength);  // once per column header
    writeByte((repeating ? IS_REPEATING : 0)
        | (column instanceof Decimal64ColumnVector ? IS_DECIMAL_64 : 0)
        | (hasNulls ? HAS_NULLS : 0));

    int nullBitmapOffset = -1;
    int nonNullCount = valueCount;
    if (hasNulls) {
      nullBitmapOffset = position;
      Arrays.fill(buffer, position, position + nullBitmapLength, (byte) 0);
      position += nullBitmapLength;
      buffer[nullBitmapOffset + (firstNull >>> 3)] |= 1 << (firstNull & 7);
      nonNullCount--;
      for (int logical = firstNull + 1; logical < valueCount; logical++) {
        if (column.isNull[physicalIndex(indices, rowOffset, logical, repeating)]) {
          buffer[nullBitmapOffset + (logical >>> 3)] |= 1 << (logical & 7);
          nonNullCount--;
        }
      }
    }

    if (column instanceof StructColumnVector) {
      writeStructChildren((StructColumnVector) column, indices, rowOffset, valueCount, repeating,
          nullBitmapOffset);
    } else if (column instanceof UnionColumnVector) {
      writeUnionChildren((UnionColumnVector) column, indices, rowOffset, valueCount, repeating,
          nullBitmapOffset, nonNullCount);
    } else if (column instanceof ListColumnVector) {
      ListColumnVector list = (ListColumnVector) column;
      writeMultiValuedChildren(list, list.child, null, indices, rowOffset, valueCount, repeating,
          nullBitmapOffset, nonNullCount);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      writeMultiValuedChildren(map, map.keys, map.values, indices, rowOffset, valueCount, repeating,
          nullBitmapOffset, nonNullCount);
    } else {
      writeValue(column, indices, rowOffset, valueCount, repeating, nullBitmapOffset, nonNullCount);
    }
  }

  private void writeStructChildren(StructColumnVector struct, int[] indices, int rowOffset,
      int valueCount, boolean repeating, int nullBitmapOffset) {
    if (nullBitmapOffset < 0) {
      for (ColumnVector field : struct.fields) {
        writeColumn(field, repeating ? null : indices, repeating ? 0 : rowOffset,
            valueCount, true);
      }
      return;
    }

    int activeCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        activeCount++;
      }
    }

    int[] activeIndices = new int[activeCount];
    int activePosition = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        activeIndices[activePosition++] = physicalIndex(indices, rowOffset, logical, repeating);
      }
    }

    for (ColumnVector field : struct.fields) {
      writeColumn(field, activeIndices, 0, activeCount, true);
    }
  }

  private void writeUnionChildren(UnionColumnVector union, int[] indices, int rowOffset,
      int valueCount, boolean repeating, int nullBitmapOffset, int nonNullCount) {
    ensureCapacity((long) nonNullCount * Integer.BYTES);  // once per union-tag section
    int[] fieldCounts = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        int tag = union.tags[physicalIndex(indices, rowOffset, logical, repeating)];
        validateUnionTag(tag, union.fields.length);
        writeInt(tag);
        fieldCounts[tag]++;
      }
    }

    int[][] fieldIndices = new int[union.fields.length][];
    for (int tag = 0; tag < union.fields.length; tag++) {
      fieldIndices[tag] = new int[fieldCounts[tag]];
    }
    int[] fieldPositions = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        int index = physicalIndex(indices, rowOffset, logical, repeating);
        int tag = union.tags[index];
        fieldIndices[tag][fieldPositions[tag]++] = index;
      }
    }

    for (int tag = 0; tag < union.fields.length; tag++) {
      writeColumn(union.fields[tag], fieldIndices[tag], 0, fieldCounts[tag], true);
    }
  }

  private void validateUnionTag(int tag, int fieldCount) {
    if (tag < 0 || tag >= fieldCount) {
      throw new IllegalArgumentException(
          "Invalid union tag " + tag + " for " + fieldCount + " fields");
    }
  }

  private void writeMultiValuedChildren(MultiValuedColumnVector parent, ColumnVector firstChild,
      ColumnVector secondChild, int[] indices, int rowOffset, int valueCount, boolean repeating,
      int nullBitmapOffset, int nonNullCount) {
    ensureCapacity((long) nonNullCount * Integer.BYTES);  // once per list/map length section
    int childCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        int index = physicalIndex(indices, rowOffset, logical, repeating);
        int length = Math.toIntExact(parent.lengths[index]);
        writeInt(length);
        childCount = Math.addExact(childCount, length);
      }
    }

    int[] childIndices = new int[childCount];
    int childPosition = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (!isNull(nullBitmapOffset, logical)) {
        int index = physicalIndex(indices, rowOffset, logical, repeating);
        int offset = Math.toIntExact(parent.offsets[index]);
        int length = Math.toIntExact(parent.lengths[index]);
        for (int child = 0; child < length; child++) {
          childIndices[childPosition++] = offset + child;
        }
      }
    }
    writeColumn(firstChild, childIndices, 0, childCount, true);
    if (secondChild != null) {
      writeColumn(secondChild, childIndices, 0, childCount, true);
    }
  }

  private void reset(byte[] buffer, int position) {
    assert position >= 0 && position <= buffer.length;
    this.buffer = buffer;
    this.position = position;
  }

  // Callers reserve the complete header or payload before these unchecked writes.
  private void writeByte(int value) {
    buffer[position++] = (byte) value;
  }

  private void writeInt(int value) {
    theUnsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET + position, value);
    position += Integer.BYTES;
  }

  private void writeLong(long value) {
    theUnsafe.putLong(buffer, BYTE_ARRAY_BASE_OFFSET + position, value);
    position += Long.BYTES;
  }

  private void writeDouble(double value) {
    writeLong(Double.doubleToLongBits(value));
  }

  private void ensureCapacity(long length) {
    if (length < 0 || length > buffer.length - (long) position) {
      throw new ArrayIndexOutOfBoundsException(
          "position=" + position + ", length=" + length + ", limit=" + buffer.length);
    }
  }

  private int physicalIndex(int[] indices, int rowOffset, int logical, boolean repeating) {
    return repeating ? 0 : indices == null ? rowOffset + logical : indices[rowOffset + logical];
  }

  private void writeValue(ColumnVector column, int[] indices, int rowOffset, int valueCount,
      boolean repeating, int nullBitmapOffset, int nonNullCount) {
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytes = (BytesColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmapOffset, logical)) {
          int index = physicalIndex(indices, rowOffset, logical, repeating);
          int length = bytes.length[index];
          if (length < 0) {
            throw new IllegalArgumentException("Negative byte length: " + length);
          }
          // Copy first: arraycopy validates space for the payload and preceding length before unchecked putInt.
          int valueOffset = position + Integer.BYTES;
          System.arraycopy(bytes.vector[index], bytes.start[index], buffer, valueOffset, length);
          theUnsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET + position, length);
          position = valueOffset + length;
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      ensureCapacity((long) nonNullCount * (Long.BYTES + Integer.BYTES));   // once per timestamp column
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmapOffset, logical)) {
          int index = physicalIndex(indices, rowOffset, logical, repeating);
          writeLong(timestamp.time[index]);
          writeInt(timestamp.nanos[index]);
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      IntervalDayTimeColumnVector intervalColumn = (IntervalDayTimeColumnVector) column;
      ensureCapacity((long) nonNullCount * (Long.BYTES + Integer.BYTES));   // once per interval day-time column
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmapOffset, logical)) {
          HiveIntervalDayTime interval =
              intervalColumn.asScratchIntervalDayTime(
                  physicalIndex(indices, rowOffset, logical, repeating));
          writeLong(interval.getTotalSeconds());
          writeInt(interval.getNanos());
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmapOffset, logical)) {
          position += decimal
            .vector[physicalIndex(indices, rowOffset, logical, repeating)]
            .writeDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Includes Decimal64ColumnVector and DateColumnVector: all use native-endian longs.
      LongColumnVector longs = (LongColumnVector) column;
      ensureCapacity((long) nonNullCount * Long.BYTES);   // once per long column
      if (nullBitmapOffset < 0 && !repeating) {
        int bytes = valueCount * Long.BYTES;
        if (indices == null) {
          VectorShuffleBatchFixedWidth.writeLongs(
              longs.vector, rowOffset, buffer, position, valueCount);
        } else {
          final long base = BYTE_ARRAY_BASE_OFFSET + position;
          for (int i = 0; i < valueCount; i++) {
            theUnsafe.putLong(buffer, base + (long) i * Long.BYTES,
                longs.vector[indices[rowOffset + i]]);
          }
        }
        position += bytes;
      } else {
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmapOffset, logical)) {
            writeLong(longs.vector[physicalIndex(indices, rowOffset, logical, repeating)]);
          }
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      ensureCapacity((long) nonNullCount * Double.BYTES);   // once per double column
      if (nullBitmapOffset < 0 && !repeating) {
        int bytes = valueCount * Double.BYTES;
        // Do not bulk-copy raw doubles: doubleToLongBits canonicalizes NaN payloads.
        final long base = BYTE_ARRAY_BASE_OFFSET + position;
        final double[] values = doubles.vector;
        if (indices == null) {
          for (int i = 0; i < valueCount; i++) {
            theUnsafe.putLong(buffer, base + (long) i * Double.BYTES,
                Double.doubleToLongBits(values[rowOffset + i]));
          }
        } else {
          for (int i = 0; i < valueCount; i++) {
            theUnsafe.putLong(buffer, base + (long) i * Double.BYTES,
                Double.doubleToLongBits(values[indices[rowOffset + i]]));
          }
        }
        position += bytes;
      } else {
        for (int logical = 0; logical < valueCount; logical++) {
          if (!isNull(nullBitmapOffset, logical)) {
            writeDouble(doubles.vector[physicalIndex(indices, rowOffset, logical, repeating)]);
          }
        }
      }
    } else if (column instanceof VoidColumnVector) {
      // VOID has no value payload.
    } else {
      throw unsupported(column);
    }
  }

  private boolean isNull(int nullBitmapOffset, int logical) {
    return nullBitmapOffset >= 0
        && (buffer[nullBitmapOffset + (logical >>> 3)] & (1 << (logical & 7))) != 0;
  }

  private IllegalArgumentException unsupported(ColumnVector column) {
    return new IllegalArgumentException(
        "Unsupported vector shuffle column type " + column.getClass().getName());
  }
}
