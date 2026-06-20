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
import org.apache.hadoop.io.BytesWritable;

/**
 * Serializes the active rows of selected columns from a {@link VectorizedRowBatch}.
 *
 * <p>The receiver is expected to know the column schema. Selected rows
 * are compacted in logical order, inactive rows and unselected columns are omitted, null values are
 * omitted, and repeating columns are represented by a single value. Primitive, list, map, struct, and
 * union vectors are supported recursively.</p>
 */
public final class VectorShuffleBatchSerializer {
  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;
  private static final int IS_DECIMAL_64 = 4;

  private static final int INITIAL_BUFFER_SIZE = 102400;

  private final byte[] buffer = new byte[INITIAL_BUFFER_SIZE];
  private int position;

  public void serialize(VectorizedRowBatch source, int[] sourceColumnMap, BytesWritable output) {
    int[] logicalRows = new int[source.size];
    for (int logical = 0; logical < source.size; logical++) {
      logicalRows[logical] = source.selectedInUse ? source.selected[logical] : logical;
    }

    reset();
    writeInt(source.size);
    writeInt(sourceColumnMap.length);
    for (int sourceColumn : sourceColumnMap) {
      writeColumn(source.cols[sourceColumn], logicalRows, source.size);
    }
    output.set(buffer, 0, position);
  }

  public void serialize(VectorizedRowBatch source, int[] sourceColumnMap, int[] rowIndices,
      int rowOffset, int rowCount, BytesWritable output) {
    assert rowOffset <= rowIndices.length - rowCount;

    int[] logicalRows = rowIndices;
    if (rowOffset != 0) {
      logicalRows = new int[rowCount];
      System.arraycopy(rowIndices, rowOffset, logicalRows, 0, rowCount);
    }

    reset();
    writeInt(rowCount);
    writeInt(sourceColumnMap.length);
    for (int sourceColumn : sourceColumnMap) {
      writeColumn(source.cols[sourceColumn], logicalRows, rowCount);
    }
    output.set(buffer, 0, position);
  }

  private void writeColumn(ColumnVector column, int[] indices, int count) {
    final boolean repeating = column.isRepeating;
    final int valueCount = repeating ? Math.min(count, 1) : count;
    final boolean hasNulls = hasNulls(column, indices, valueCount, repeating);
    writeByte((repeating ? IS_REPEATING : 0) | (hasNulls ? HAS_NULLS : 0)
        | (column instanceof Decimal64ColumnVector ? IS_DECIMAL_64 : 0));

    byte[] nullBitmap = null;
    if (hasNulls) {
      nullBitmap = new byte[(valueCount + 7) / 8];
      for (int logical = 0; logical < valueCount; logical++) {
        if (isNull(column, indices, logical, repeating)) {
          nullBitmap[logical >>> 3] |= 1 << (logical & 7);
        }
      }
      writeBytes(nullBitmap);
    }

    writeTypeMetadata(column);
    if (column instanceof StructColumnVector) {
      writeStructChildren((StructColumnVector) column, indices, valueCount, repeating,
          nullBitmap);
    } else if (column instanceof UnionColumnVector) {
      writeUnionChildren((UnionColumnVector) column, indices, valueCount, repeating, nullBitmap);
    } else if (column instanceof ListColumnVector) {
      ListColumnVector list = (ListColumnVector) column;
      writeMultiValuedChildren(list, list.child, null, indices, valueCount, repeating, nullBitmap);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      writeMultiValuedChildren(map, map.keys, map.values, indices, valueCount, repeating, nullBitmap);
    } else {
      writeValue(column, indices, valueCount, repeating, nullBitmap);
    }
  }

  private void writeStructChildren(StructColumnVector struct, int[] indices, int valueCount,
      boolean repeating, byte[] nullBitmap) {
    int activeCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        activeCount++;
      }
    }

    int[] activeIndices = new int[activeCount];
    int activePosition = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        activeIndices[activePosition++] = physicalIndex(indices, logical, repeating);
      }
    }

    for (ColumnVector field : struct.fields) {
      writeColumn(field, activeIndices, activeCount);
    }
  }

  private void writeUnionChildren(UnionColumnVector union, int[] indices, int valueCount,
      boolean repeating, byte[] nullBitmap) {
    int[] fieldCounts = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int tag = union.tags[physicalIndex(indices, logical, repeating)];
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
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int index = physicalIndex(indices, logical, repeating);
        int tag = union.tags[index];
        fieldIndices[tag][fieldPositions[tag]++] = index;
      }
    }

    for (int tag = 0; tag < union.fields.length; tag++) {
      writeColumn(union.fields[tag], fieldIndices[tag], fieldCounts[tag]);
    }
  }

  private void validateUnionTag(int tag, int fieldCount) {
    if (tag < 0 || tag >= fieldCount) {
      throw new IllegalArgumentException(
          "Invalid union tag " + tag + " for " + fieldCount + " fields");
    }
  }

  private void writeMultiValuedChildren(MultiValuedColumnVector parent, ColumnVector firstChild,
      ColumnVector secondChild, int[] indices, int valueCount, boolean repeating, byte[] nullBitmap) {
    int childCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int index = physicalIndex(indices, logical, repeating);
        int length = Math.toIntExact(parent.lengths[index]);
        writeInt(length);
        childCount = Math.addExact(childCount, length);
      }
    }

    int[] childIndices = new int[childCount];
    int childPosition = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int index = physicalIndex(indices, logical, repeating);
        int offset = Math.toIntExact(parent.offsets[index]);
        int length = Math.toIntExact(parent.lengths[index]);
        for (int child = 0; child < length; child++) {
          childIndices[childPosition++] = offset + child;
        }
      }
    }
    writeColumn(firstChild, childIndices, childCount);
    if (secondChild != null) {
      writeColumn(secondChild, childIndices, childCount);
    }
  }

  private void reset() {
    position = 0;
  }

  private void writeByte(int value) {
    buffer[position++] = (byte) value;
  }

  private void writeBoolean(boolean value) {
    writeByte(value ? 1 : 0);
  }

  private void writeInt(int value) {
    buffer[position++] = (byte) (value >>> 24);
    buffer[position++] = (byte) (value >>> 16);
    buffer[position++] = (byte) (value >>> 8);
    buffer[position++] = (byte) value;
  }

  private void writeLong(long value) {
    buffer[position++] = (byte) (value >>> 56);
    buffer[position++] = (byte) (value >>> 48);
    buffer[position++] = (byte) (value >>> 40);
    buffer[position++] = (byte) (value >>> 32);
    buffer[position++] = (byte) (value >>> 24);
    buffer[position++] = (byte) (value >>> 16);
    buffer[position++] = (byte) (value >>> 8);
    buffer[position++] = (byte) value;
  }

  private void writeDouble(double value) {
    writeLong(Double.doubleToLongBits(value));
  }

  private void writeBytes(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  private void writeBytes(byte[] bytes, int offset, int length) {
    System.arraycopy(bytes, offset, buffer, position, length);
    position += length;
  }

  private boolean hasNulls(ColumnVector column, int[] indices, int valueCount,
      boolean repeating) {
    if (column.noNulls) {
      return false;
    }
    for (int logical = 0; logical < valueCount; logical++) {
      if (isNull(column, indices, logical, repeating)) {
        return true;
      }
    }
    return false;
  }

  private boolean isNull(ColumnVector column, int[] indices, int logical, boolean repeating) {
    return column.isNull[physicalIndex(indices, logical, repeating)];
  }

  private int physicalIndex(int[] indices, int logical, boolean repeating) {
    return repeating ? 0 : indices[logical];
  }

  private void writeTypeMetadata(ColumnVector column) {
    if (column instanceof DateColumnVector) {
      writeBoolean(((DateColumnVector) column).isUsingProlepticCalendar());
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      writeBoolean(timestamp.isUTC());
      writeBoolean(timestamp.usingProlepticCalendar());
    }
  }

  private void writeValue(ColumnVector column, int[] indices, int valueCount, boolean repeating,
      byte[] nullBitmap) {
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytes = (BytesColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          int index = physicalIndex(indices, logical, repeating);
          writeInt(bytes.length[index]);
          writeBytes(bytes.vector[index], bytes.start[index], bytes.length[index]);
        }
      }
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          int index = physicalIndex(indices, logical, repeating);
          writeLong(timestamp.time[index]);
          writeInt(timestamp.nanos[index]);
        }
      }
    } else if (column instanceof IntervalDayTimeColumnVector) {
      IntervalDayTimeColumnVector intervalColumn = (IntervalDayTimeColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          HiveIntervalDayTime interval =
              intervalColumn.asScratchIntervalDayTime(physicalIndex(indices, logical, repeating));
          writeLong(interval.getTotalSeconds());
          writeInt(interval.getNanos());
        }
      }
    } else if (column instanceof DecimalColumnVector) {
      DecimalColumnVector decimal = (DecimalColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          position += decimal.vector[physicalIndex(indices, logical, repeating)]
              .writeDirect(buffer, position);
        }
      }
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector extends LongColumnVector, so this branch covers it.
      LongColumnVector longs = (LongColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          writeLong(longs.vector[physicalIndex(indices, logical, repeating)]);
        }
      }
    } else if (column instanceof DoubleColumnVector) {
      DoubleColumnVector doubles = (DoubleColumnVector) column;
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          writeDouble(doubles.vector[physicalIndex(indices, logical, repeating)]);
        }
      }
    } else if (column instanceof VoidColumnVector) {
      // VOID has no value payload.
    } else {
      throw unsupported(column);
    }
  }

  private boolean isNull(byte[] nullBitmap, int logical) {
    return nullBitmap != null && (nullBitmap[logical >>> 3] & (1 << (logical & 7))) != 0;
  }

  private IllegalArgumentException unsupported(ColumnVector column) {
    return new IllegalArgumentException(
        "Unsupported vector shuffle column type " + column.getClass().getName());
  }
}
