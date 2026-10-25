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

import org.apache.hadoop.io.BytesWritable;

/**
 * Reusable view of a compact shuffle frame's fixed-schema header. No column values are read.
 *
 * <p>The layout is row count, column count, repeating bitmap, nested-repeating byte, metadata
 * bitmap, optional repeating-section length, repeating payloads, ordinary payloads. Ints retain
 * the scalar payload's native byte order. Bitmap bit {@code i} is bit {@code i % 8} of byte
 * {@code i / 8}. Repeating bits use logical input column order, and both payload sections retain
 * that order. Only effectively repeating top-level columns (more than one row) enter the first
 * section. Its single length is present iff any repeating bit is set.</p>
 *
 * <p>Metadata bits follow depth-first schema order, independent of payload order and child counts:
 * top-level logical columns; list child; map keys then values; struct/union fields in field order.
 * Date contributes its proleptic-calendar bit; timestamp contributes UTC then proleptic-calendar.
 * Padding bits are zero. The fixed schema determines the metadata bitmap's length.</p>
 */
public final class VectorShuffleBatchHeader {
  private byte[] buffer;
  private int rowCount;
  private int columnCount;
  private int bitmapOffset;
  private int bitmapLength;
  private int metadataOffset;
  private int metadataLength;
  private int repeatingStart;
  private int repeatingEnd;
  private int limit;
  private boolean repeating;
  private boolean nestedRepeating;
  private long repeatingBits;

  /** Borrows the serialized bytes until {@link #clear()}; does not allocate or scan payloads. */
  public void read(BytesWritable serialized, int expectedColumnCount, int metadataByteCount)
      throws IOException {
    buffer = serialized.getBytesRaw();
    int position = serialized.getOffset();
    limit = position + serialized.getLength();
    if (serialized.getLength() < 2 * Integer.BYTES) {
      throw new IOException("Truncated vector shuffle batch header");
    }
    rowCount = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
    columnCount = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position + Integer.BYTES);
    if (rowCount < 0 || columnCount != expectedColumnCount || columnCount < 0) {
      throw new IOException("Invalid vector shuffle batch dimensions: " + rowCount + " rows, "
          + columnCount + " columns");
    }
    position += 2 * Integer.BYTES;
    bitmapOffset = position;
    bitmapLength = (columnCount + 7) / 8;
    metadataLength = metadataByteCount;
    if (bitmapLength + 1 + metadataLength > limit - position) {
      throw new IOException("Truncated vector shuffle batch header");
    }
    repeatingBits = 0;
    if (columnCount <= Long.SIZE) {
      for (int i = 0; i < bitmapLength; i++) {
        repeatingBits |= (buffer[position++] & 0xffL) << (i * Byte.SIZE);
      }
      repeating = repeatingBits != 0;
    } else {
      repeating = false;
      for (int i = 0; i < bitmapLength; i++) {
        repeating |= buffer[position++] != 0;
      }
    }
    nestedRepeating = buffer[position++] != 0;
    metadataOffset = position;
    position += metadataLength;
    int repeatingLength = 0;
    if (repeating) {
      if (position > limit - Integer.BYTES) {
        throw new IOException("Truncated vector shuffle repeating-section length");
      }
      repeatingLength = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
      position += Integer.BYTES;
      if (repeatingLength < 0 || repeatingLength > limit - position) {
        throw new IOException("Invalid vector shuffle repeating-section length " + repeatingLength);
      }
    }
    repeatingStart = position;
    repeatingEnd = position + repeatingLength;
  }

  /** Release the borrowed frame, preserving small header fields for the caller's decisions. */
  public void clear() {
    buffer = null;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getColumnCount() {
    return columnCount;
  }

  public int getBitmapOffset() {
    return bitmapOffset;
  }

  public int getBitmapLength() {
    return bitmapLength;
  }

  public int getMetadataOffset() {
    return metadataOffset;
  }

  public int getMetadataLength() {
    return metadataLength;
  }

  public int getRepeatingStart() {
    return repeatingStart;
  }

  public int getRepeatingEnd() {
    return repeatingEnd;
  }

  public int getOrdinaryStart() {
    return repeatingEnd;
  }

  public int getLimit() {
    return limit;
  }

  public boolean hasRepeatingColumns() {
    return repeating;
  }

  public boolean hasNestedRepeating() {
    return nestedRepeating;
  }

  /** Logical-column bitmap for schemas of at most 64 columns, independent of native byte order. */
  public long getRepeatingBits() {
    assert columnCount <= Long.SIZE;
    return repeatingBits;
  }

  public boolean isRepeating(int logicalColumn) {
    assert logicalColumn >= 0 && logicalColumn < columnCount;
    return bit(buffer, bitmapOffset, logicalColumn);
  }

  public static int metadataBitCount(ColumnVector[] columns, int columnCount) {
    int result = 0;
    for (int i = 0; i < columnCount; i++) {
      result += metadataBitCount(columns[i]);
    }
    return result;
  }

  public static int metadataBitCount(ColumnVector column) {
    if (column instanceof DateColumnVector) {
      return 1;
    } else if (column instanceof TimestampColumnVector) {
      return 2;
    } else if (column instanceof ListColumnVector) {
      return metadataBitCount(((ListColumnVector) column).child);
    } else if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      return metadataBitCount(map.keys) + metadataBitCount(map.values);
    } else if (column instanceof StructColumnVector) {
      ColumnVector[] fields = ((StructColumnVector) column).fields;
      return metadataBitCount(fields, fields.length);
    } else if (column instanceof UnionColumnVector) {
      ColumnVector[] fields = ((UnionColumnVector) column).fields;
      return metadataBitCount(fields, fields.length);
    }
    return 0;
  }

  private static boolean bit(byte[] bytes, int offset, int bit) {
    return (bytes[offset + (bit >>> 3)] & (1 << (bit & 7))) != 0;
  }

}
