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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;

/** Deserializes a compact vector shuffle payload into an already schema-initialized batch. */
public final class VectorShuffleBatchDeserializer {
  private static final int IS_REPEATING = 1;
  private static final int HAS_NULLS = 2;

  private final DataInputBuffer input = new DataInputBuffer();

  public void deserialize(BytesWritable serialized, VectorizedRowBatch destination)
      throws IOException {
    if (serialized == null || destination == null) {
      throw new IllegalArgumentException("Serialized batch and destination are required");
    }

    input.reset(serialized.getBytes(), serialized.getLength());
    final int rowCount = WritableUtils.readVInt(input);
    final int columnCount = WritableUtils.readVInt(input);
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
    if (input.getPosition() != serialized.getLength()) {
      throw new IOException("Vector shuffle batch has "
          + (serialized.getLength() - input.getPosition()) + " trailing bytes");
    }
  }

  private void readColumn(ColumnVector column, int rowCount) throws IOException {
    readColumn(column, null, rowCount);
  }

  private void readColumn(ColumnVector column, int[] destinationIndices, int rowCount)
      throws IOException {
    final int flags = input.readUnsignedByte();
    final boolean repeating = (flags & IS_REPEATING) != 0;
    final boolean hasNulls = (flags & HAS_NULLS) != 0;
    final int valueCount = repeating ? Math.min(rowCount, 1) : rowCount;
    column.ensureSize(requiredSize(destinationIndices, valueCount, repeating), false);

    byte[] nullBitmap = null;
    if (hasNulls) {
      nullBitmap = new byte[(valueCount + 7) / 8];
      input.readFully(nullBitmap);
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
      ensurePrimitiveSupported(column);
      for (int logical = 0; logical < valueCount; logical++) {
        if (!isNull(nullBitmap, logical)) {
          readValue(column, destinationIndex(destinationIndices, logical, repeating));
        }
      }
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
        int tag = WritableUtils.readVInt(input);
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
        int length = WritableUtils.readVInt(input);
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
      ((DateColumnVector) column).setUsingProlepticCalendar(input.readBoolean());
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      timestamp.setIsUTC(input.readBoolean());
      timestamp.setUsingProlepticCalendar(input.readBoolean());
    }
  }

  private void readValue(ColumnVector column, int index) throws IOException {
    if (column instanceof BytesColumnVector) {
      int length = WritableUtils.readVInt(input);
      if (length < 0) {
        throw new IOException("Negative byte-vector value length " + length);
      }
      byte[] bytes = new byte[length];
      input.readFully(bytes);
      ((BytesColumnVector) column).setVal(index, bytes);
    } else if (column instanceof TimestampColumnVector) {
      TimestampColumnVector timestamp = (TimestampColumnVector) column;
      timestamp.time[index] = input.readLong();
      timestamp.nanos[index] = input.readInt();
    } else if (column instanceof IntervalDayTimeColumnVector) {
      ((IntervalDayTimeColumnVector) column).set(index,
          new HiveIntervalDayTime(input.readLong(), input.readInt()));
    } else if (column instanceof DecimalColumnVector) {
      ((DecimalColumnVector) column).vector[index].readFields(input);
    } else if (column instanceof LongColumnVector) {
      ((LongColumnVector) column).vector[index] = input.readLong();
    } else if (column instanceof DoubleColumnVector) {
      ((DoubleColumnVector) column).vector[index] = input.readDouble();
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
