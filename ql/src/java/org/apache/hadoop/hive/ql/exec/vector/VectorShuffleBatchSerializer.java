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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

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

  private final DataOutputBuffer buffer = new DataOutputBuffer();

  public void serialize(VectorizedRowBatch source, int[] sourceColumnMap, BytesWritable output)
      throws IOException {
    if (source == null || sourceColumnMap == null || output == null) {
      throw new IllegalArgumentException("Source batch, source column map, and output are required");
    }

    int[] logicalRows = new int[source.size];
    for (int logical = 0; logical < source.size; logical++) {
      logicalRows[logical] = source.selectedInUse ? source.selected[logical] : logical;
    }

    buffer.reset();
    WritableUtils.writeVInt(buffer, source.size);
    WritableUtils.writeVInt(buffer, sourceColumnMap.length);
    for (int sourceColumn : sourceColumnMap) {
      if (sourceColumn < 0 || sourceColumn >= source.cols.length || source.cols[sourceColumn] == null) {
        throw new IllegalArgumentException("Invalid source column " + sourceColumn);
      }
      writeColumn(source.cols[sourceColumn], logicalRows, source.size);
    }
    output.set(buffer.getData(), 0, buffer.getLength());
  }

  public void serialize(VectorizedRowBatch source, int[] sourceColumnMap, int[] rowIndices,
      int rowOffset, int rowCount, BytesWritable output) throws IOException {
    if (source == null || sourceColumnMap == null || rowIndices == null || output == null) {
      throw new IllegalArgumentException(
          "Source batch, source column map, row indices, and output are required");
    }
    if (rowOffset < 0 || rowCount < 0 || rowOffset > rowIndices.length - rowCount) {
      throw new IllegalArgumentException(
          "Invalid row offset " + rowOffset + " and row count " + rowCount
              + " for " + rowIndices.length + " row indices");
    }
    int[] logicalRows = rowIndices;
    if (rowOffset != 0) {
      logicalRows = new int[rowCount];
      System.arraycopy(rowIndices, rowOffset, logicalRows, 0, rowCount);
    }

    buffer.reset();
    WritableUtils.writeVInt(buffer, rowCount);
    WritableUtils.writeVInt(buffer, sourceColumnMap.length);
    for (int sourceColumn : sourceColumnMap) {
      if (sourceColumn < 0 || sourceColumn >= source.cols.length || source.cols[sourceColumn] == null) {
        throw new IllegalArgumentException("Invalid source column " + sourceColumn);
      }
      writeColumn(source.cols[sourceColumn], logicalRows, rowCount);
    }
    output.set(buffer.getData(), 0, buffer.getLength());
  }

  private void writeColumn(ColumnVector column, int[] indices, int count) throws IOException {
    final boolean repeating = column.isRepeating;
    final int valueCount = repeating ? Math.min(count, 1) : count;
    final boolean hasNulls = hasNulls(column, indices, valueCount, repeating);
    buffer.writeByte((repeating ? IS_REPEATING : 0) | (hasNulls ? HAS_NULLS : 0)
        | (column instanceof Decimal64ColumnVector ? IS_DECIMAL_64 : 0));

    byte[] nullBitmap = null;
    if (hasNulls) {
      nullBitmap = new byte[(valueCount + 7) / 8];
      for (int logical = 0; logical < valueCount; logical++) {
        if (isNull(column, indices, logical, repeating)) {
          nullBitmap[logical >>> 3] |= 1 << (logical & 7);
        }
      }
      buffer.write(nullBitmap);
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
      ensurePrimitiveSupported(column);
      for (int logical = 0; logical < valueCount; logical++) {
        if (!hasNulls || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
          writeValue(column, physicalIndex(indices, logical, repeating));
        }
      }
    }
  }


  private void writeStructChildren(StructColumnVector struct, int[] indices, int valueCount,
      boolean repeating, byte[] nullBitmap) throws IOException {
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
      boolean repeating, byte[] nullBitmap) throws IOException {
    int[] fieldCounts = new int[union.fields.length];
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int tag = union.tags[physicalIndex(indices, logical, repeating)];
        validateUnionTag(tag, union.fields.length);
        WritableUtils.writeVInt(buffer, tag);
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
      ColumnVector secondChild, int[] indices, int valueCount, boolean repeating, byte[] nullBitmap)
      throws IOException {
    int childCount = 0;
    for (int logical = 0; logical < valueCount; logical++) {
      if (nullBitmap == null || (nullBitmap[logical >>> 3] & (1 << (logical & 7))) == 0) {
        int index = physicalIndex(indices, logical, repeating);
        int length = Math.toIntExact(parent.lengths[index]);
        WritableUtils.writeVInt(buffer, length);
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

  private void writeTypeMetadata(ColumnVector column) throws IOException {
    if (column instanceof DateColumnVector) {
      buffer.writeBoolean(((DateColumnVector) column).isUsingProlepticCalendar());
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      buffer.writeBoolean(timestamp.isUTC());
      buffer.writeBoolean(timestamp.usingProlepticCalendar());
    }
  }

  private void writeValue(ColumnVector column, int index) throws IOException {
    if (column instanceof BytesColumnVector) {
      BytesColumnVector bytes = (BytesColumnVector) column;
      WritableUtils.writeVInt(buffer, bytes.length[index]);
      buffer.write(bytes.vector[index], bytes.start[index], bytes.length[index]);
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      buffer.writeLong(timestamp.time[index]);
      buffer.writeInt(timestamp.nanos[index]);
    } else if (column instanceof IntervalDayTimeColumnVector) {
      HiveIntervalDayTime interval =
          ((IntervalDayTimeColumnVector) column).asScratchIntervalDayTime(index);
      buffer.writeLong(interval.getTotalSeconds());
      buffer.writeInt(interval.getNanos());
    } else if (column instanceof DecimalColumnVector) {
      ((DecimalColumnVector) column).vector[index].write(buffer);
    } else if (column instanceof LongColumnVector) {
      // Decimal64ColumnVector extends LongColumnVector, so this branch covers it.
      buffer.writeLong(((LongColumnVector) column).vector[index]);
    } else if (column instanceof DoubleColumnVector) {
      buffer.writeDouble(((DoubleColumnVector) column).vector[index]);
    } else if (column instanceof VoidColumnVector) {
      // VOID has no value payload.
    } else {
      throw unsupported(column);
    }
  }

  private void ensurePrimitiveSupported(ColumnVector column) {
    if (!(column instanceof BytesColumnVector || column instanceof TimestampColumnVector
        || column instanceof IntervalDayTimeColumnVector || column instanceof DecimalColumnVector
        || column instanceof LongColumnVector || column instanceof DoubleColumnVector
        || column instanceof VoidColumnVector)) {
      throw unsupported(column);
    }
  }

  private IllegalArgumentException unsupported(ColumnVector column) {
    return new IllegalArgumentException(
        "Unsupported vector shuffle column type " + column.getClass().getName());
  }
}
