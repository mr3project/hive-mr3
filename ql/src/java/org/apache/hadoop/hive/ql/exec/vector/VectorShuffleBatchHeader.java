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
 * The first byte contains format flags. A single-row frame contains the metadata bitmap followed
 * by ordinary column payloads. Its column count comes from the fixed schema, and its nested
 * repeating flag is carried in the format flags.
 *
 * For other frames, the common prefix is column count, repeating bitmap, metadata bitmap, optional
 * repeating-section length, and repeating payloads. The partition-specific suffix is row count,
 * nested-repeating byte, and ordinary payloads. The nested-repeating byte summarizes both payload
 * sections. The repeating-section length locates the suffix without scanning payloads. Ints retain
 * the scalar payload's native byte order. Bitmap bit {@code i} is bit {@code i % 8} of byte
 * {@code i / 8}. Repeating bits use logical input column order, and both payload sections retain
 * that order. Only effectively repeating top-level columns (more than one row) enter the first
 * section. Its single length is present iff any repeating bit is set. This layout replaces the
 * row-count-first format; producers and receivers must use the same format.
 *
 * Metadata bits follow depth-first schema order, independent of payload order and child counts:
 * top-level logical columns; list child; map keys then values; struct/union fields in field order.
 * Date contributes its proleptic-calendar bit; timestamp contributes UTC then proleptic-calendar.
 * Padding bits are zero. The fixed schema determines the metadata bitmap's length.
 */
public final class VectorShuffleBatchHeader {

  public static final int SINGLE_ROW = 1;
  public static final int SINGLE_ROW_NESTED_REPEATING = 2;

  private static final int PARTITION_HEADER_SIZE = Integer.BYTES + Byte.BYTES;

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
  private boolean singleRow;
  private long repeatingBits;

  public void read(BytesWritable serialized, int expectedColumnCount, int metadataByteCount)
      throws IOException {
    buffer = serialized.getBytesRaw();
    int position = serialized.getOffset();
    limit = position + serialized.getLength();
    if (serialized.getLength() < Byte.BYTES) {
      throw new IOException("Truncated vector shuffle batch header");
    }

    int formatFlags = buffer[position++] & 0xff;
    singleRow = (formatFlags & SINGLE_ROW) != 0;
    if (singleRow) {
      columnCount = expectedColumnCount;
    } else {
      if (limit - position < Integer.BYTES) {
        throw new IOException("Truncated vector shuffle batch header");
      }
      columnCount = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
      if (columnCount != expectedColumnCount) {
        throw new IOException("Invalid vector shuffle batch column count " + columnCount);
      }
      position += Integer.BYTES;
    }

    bitmapOffset = position;
    bitmapLength = singleRow ? 0 : (int) ((columnCount + 7L) / 8);
    metadataLength = metadataByteCount;
    if ((long) bitmapLength + metadataLength + (singleRow ? 0 : PARTITION_HEADER_SIZE)
        > limit - position) {
      throw new IOException("Truncated vector shuffle batch header");
    }

    repeatingBits = 0;
    if (singleRow) {
      metadataOffset = position;
      repeatingStart = position + metadataLength;
      repeatingEnd = repeatingStart;
      rowCount = 1;
      repeating = false;
      nestedRepeating = (formatFlags & SINGLE_ROW_NESTED_REPEATING) != 0;
      return;
    }

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

    metadataOffset = position;

    position += metadataLength;
    int repeatingLength = 0;
    if (repeating) {
      if (position > limit - Integer.BYTES - PARTITION_HEADER_SIZE) {
        throw new IOException("Truncated vector shuffle repeating-section length");
      }
      repeatingLength = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + position);
      position += Integer.BYTES;
      if (repeatingLength < 0 || repeatingLength > limit - position - PARTITION_HEADER_SIZE) {
        throw new IOException("Invalid vector shuffle repeating-section length " + repeatingLength);
      }
    }

    repeatingStart = position;
    repeatingEnd = position + repeatingLength;
    rowCount = theUnsafe.getInt(buffer, BYTE_ARRAY_BASE_OFFSET + repeatingEnd);
    if (rowCount < 0) {
      throw new IOException("Invalid vector shuffle batch row count " + rowCount);
    }

    nestedRepeating = buffer[repeatingEnd + Integer.BYTES] != 0;
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
    return singleRow ? repeatingEnd : repeatingEnd + PARTITION_HEADER_SIZE;
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
    return !singleRow && bit(buffer, bitmapOffset, logicalColumn);
  }

  private static boolean bit(byte[] bytes, int offset, int bit) {
    return (bytes[offset + (bit >>> 3)] & (1 << (bit & 7))) != 0;
  }

}
