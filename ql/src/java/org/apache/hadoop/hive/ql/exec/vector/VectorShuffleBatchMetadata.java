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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fixed-schema metadata layout. Descriptors retain schema paths, never a batch's vectors, so
 * replacement batches and nested vectors use the same layout without another schema traversal.
 */
public final class VectorShuffleBatchMetadata {
  private static final int LIST_CHILD = Integer.MIN_VALUE;
  private static final int MAP_KEYS = Integer.MIN_VALUE + 1;
  private static final int MAP_VALUES = Integer.MIN_VALUE + 2;
  private static final int[] ROOT = new int[0];

  private final Descriptor[] descriptors;
  private final int bitCount;
  private final int byteCount;

  public VectorShuffleBatchMetadata(ColumnVector[] columns, int columnCount) {
    List<Descriptor> layout = new ArrayList<>();
    int bits = 0;
    for (int i = 0; i < columnCount; i++) {
      bits = collect(columns[i], i, ROOT, bits, layout);
    }
    descriptors = layout.toArray(new Descriptor[0]);
    bitCount = bits;
    byteCount = (bits + 7) / 8;
  }

  public VectorShuffleBatchMetadata(ColumnVector[] columns, int[] columnMap) {
    List<Descriptor> layout = new ArrayList<>();
    int bits = 0;
    for (int column : columnMap) {
      bits = collect(columns[column], column, ROOT, bits, layout);
    }
    descriptors = layout.toArray(new Descriptor[0]);
    bitCount = bits;
    byteCount = (bits + 7) / 8;
  }

  public int getBitCount() {
    return bitCount;
  }

  public int getByteCount() {
    return byteCount;
  }

  /** Write flags in depth-first schema order, including children with no serialized values. */
  public void write(ColumnVector[] columns, byte[] bytes, int offset) {
    Arrays.fill(bytes, offset, offset + byteCount, (byte) 0);
    for (Descriptor descriptor : descriptors) {
      ColumnVector column = descriptor.resolve(columns);
      if (descriptor.timestamp) {
        TimestampColumnVector timestamp = (TimestampColumnVector) column;
        if (timestamp.isUTC()) {
          setBit(bytes, offset, descriptor.bit);
        }
        if (timestamp.usingProlepticCalendar()) {
          int calendarBit = descriptor.bit + 1;
          setBit(bytes, offset, calendarBit);
        }
      } else if (((DateColumnVector) column).isUsingProlepticCalendar()) {
        setBit(bytes, offset, descriptor.bit);
      }
    }
  }

  /** Install flags without converting the date/timestamp values in the batch. */
  public void apply(ColumnVector[] columns, byte[] bytes, int offset) {
    for (Descriptor descriptor : descriptors) {
      ColumnVector column = descriptor.resolve(columns);
      boolean flag = getBit(bytes, offset, descriptor.bit);
      if (descriptor.timestamp) {
        TimestampColumnVector timestamp = (TimestampColumnVector) column;
        timestamp.setIsUTC(flag);
        int calendarBit = descriptor.bit + 1;
        timestamp.setUsingProlepticCalendar(getBit(bytes, offset, calendarBit));
      } else {
        ((DateColumnVector) column).setUsingProlepticCalendar(flag);
      }
    }
  }

  /** Derive the byte offset and mask from a metadata bit index. */
  private static boolean getBit(byte[] bytes, int offset, int bit) {
    int byteOffset = bit >>> 3;
    int mask = 1 << (bit & 7);
    return (bytes[offset + byteOffset] & mask) != 0;
  }

  private static void setBit(byte[] bytes, int offset, int bit) {
    int byteOffset = bit >>> 3;
    int mask = 1 << (bit & 7);
    bytes[offset + byteOffset] |= mask;
  }

  /** Restore the metadata policy of a fresh execution batch before decoding row input. */
  public void reset(ColumnVector[] columns) {
    for (Descriptor descriptor : descriptors) {
      ColumnVector column = descriptor.resolve(columns);
      if (descriptor.timestamp) {
        TimestampColumnVector timestamp = (TimestampColumnVector) column;
        timestamp.setIsUTC(false);
        timestamp.setUsingProlepticCalendar(false);
      } else {
        ((DateColumnVector) column).setUsingProlepticCalendar(false);
      }
    }
  }

  private static int collect(ColumnVector column, int sourceColumn, int[] path, int bit,
      List<Descriptor> layout) {
    if (column instanceof DateColumnVector || column instanceof TimestampColumnVector) {
      boolean timestamp = column instanceof TimestampColumnVector;
      layout.add(new Descriptor(sourceColumn, path, bit, timestamp));
      return bit + (timestamp ? 2 : 1);
    } else if (column instanceof ListColumnVector) {
      return collect(((ListColumnVector) column).child, sourceColumn,
          childPath(path, LIST_CHILD), bit, layout);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      bit = collect(map.keys, sourceColumn, childPath(path, MAP_KEYS), bit, layout);
      return collect(map.values, sourceColumn, childPath(path, MAP_VALUES), bit, layout);
    } else if (column instanceof StructColumnVector) {
      ColumnVector[] fields = ((StructColumnVector) column).fields;
      for (int i = 0; i < fields.length; i++) {
        bit = collect(fields[i], sourceColumn, childPath(path, i), bit, layout);
      }
    } else if (column instanceof UnionColumnVector) {
      ColumnVector[] fields = ((UnionColumnVector) column).fields;
      for (int i = 0; i < fields.length; i++) {
        bit = collect(fields[i], sourceColumn, childPath(path, -i - 1), bit, layout);
      }
    }
    return bit;
  }

  private static int[] childPath(int[] path, int child) {
    int[] result = Arrays.copyOf(path, path.length + 1);
    result[path.length] = child;
    return result;
  }

  private static final class Descriptor {
    private final int sourceColumn;
    private final int[] path;
    private final boolean timestamp;
    private final int bit;

    private Descriptor(int sourceColumn, int[] path, int bit, boolean timestamp) {
      this.sourceColumn = sourceColumn;
      this.path = path;
      this.timestamp = timestamp;
      this.bit = bit;
      // For DATE, bit stores the proleptic-calendar flag. For TIMESTAMP,
      // bit stores the UTC flag and bit + 1 stores the proleptic-calendar flag.
      // getBit() and setBit() derive the byte offset and mask from either index:
      // byteOffset = bit >>> 3;
      // mask = 1 << (bit & 7);
      // calendarByteOffset = (bit + 1) >>> 3;
      // calendarMask = 1 << ((bit + 1) & 7);
    }

    private ColumnVector resolve(ColumnVector[] columns) {
      ColumnVector column = columns[sourceColumn];
      for (int child : path) {
        switch (child) {
        case LIST_CHILD:
          column = ((ListColumnVector) column).child;
          break;
        case MAP_KEYS:
          column = ((MapColumnVector) column).keys;
          break;
        case MAP_VALUES:
          column = ((MapColumnVector) column).values;
          break;
        default:
          column = child >= 0 ? ((StructColumnVector) column).fields[child]
              : ((UnionColumnVector) column).fields[-child - 1];
          break;
        }
      }
      return column;
    }
  }
}
