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
package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.io.IOException;

import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

/** Parses immutable frame slices without copying keys or values. Getter state lasts until next(). */
public final class VectorMapJoinBatchDeserializer {
  private final HashTableKeyType keyType;
  private final int numericKeyWidth;
  private final int entryHeaderBytes;
  private byte[] bytes;
  private int position;
  private int limit;
  private int rowCount;
  private int remaining;
  private long longKey;
  private boolean isNull;
  private int keyOffset;
  private int keyLength;
  private int valueOffset;
  private int valueLength;

  public VectorMapJoinBatchDeserializer(HashTableKeyType keyType) {
    numericKeyWidth = VectorMapJoinBatch.numericWidth(keyType);
    entryHeaderBytes = numericKeyWidth > 0
        ? VectorMapJoinBatch.NUMERIC_ENTRY_HEADER_BYTES : VectorMapJoinBatch.BYTES_ENTRY_HEADER_BYTES;
    this.keyType = keyType;
  }

  public void set(BytesWritable value) throws IOException {
    bytes = value.getBytesRaw();
    position = value.getOffset();
    final int length = value.getLength();
    if (length < VectorMapJoinBatch.HEADER_BYTES || length > VectorMapJoinBatch.MAX_BYTES
        || position < 0 || position > bytes.length - length) {
      throw new IOException("Invalid map-join batch slice");
    }
    limit = position + length;
    rowCount = readInt();
    if (rowCount < 1 || rowCount > VectorMapJoinBatch.MAX_ROWS
        || rowCount > (limit - position) / entryHeaderBytes) {
      throw new IOException("Invalid map-join batch row count " + rowCount);
    }
    remaining = rowCount;
  }

  public boolean next() throws IOException {
    if (remaining == 0) {
      if (position != limit) {
        throw new IOException("Trailing bytes in map-join batch");
      }
      return false;
    }
    if (limit - position < entryHeaderBytes) {
      throw new IOException("Truncated map-join batch metadata");
    }
    if (numericKeyWidth > 0) {
      isNull = bytes[position++] == 0;
      keyLength = isNull ? -1 : numericKeyWidth;
    } else {
      keyLength = readInt();
      isNull = keyLength == -1;
    }
    valueLength = readInt();
    if (keyLength < -1 || valueLength < 0
        || (isNull && keyType == HashTableKeyType.MULTI_KEY)) {
      throw new IOException("Invalid map-join batch entry lengths or NULL key");
    }
    final int payloadKeyLength = isNull ? 0 : keyLength;
    if (payloadKeyLength > limit - position
        || valueLength > limit - position - payloadKeyLength) {
      throw new IOException("Truncated map-join batch entry");
    }
    keyOffset = position;
    longKey = 0;
    if (!isNull && numericKeyWidth > 0) {
      // Seed with a signed byte to sign-extend byte/short/int as well as long keys.
      longKey = bytes[position];
      for (int i = 1; i < numericKeyWidth; i++) {
        longKey = (longKey << Byte.SIZE) | (bytes[position + i] & 0xffL);
      }
      if (keyType == HashTableKeyType.BOOLEAN && longKey != 0 && longKey != 1) {
        throw new IOException("Invalid native boolean key");
      }
    }
    position += payloadKeyLength;
    valueOffset = position;
    position += valueLength;
    remaining--;
    if (remaining == 0 && position != limit) {
      throw new IOException("Trailing bytes in map-join batch");
    }
    return true;
  }

  private int readInt() throws IOException {
    if (limit - position < Integer.BYTES) {
      throw new IOException("Truncated map-join batch metadata");
    }
    final int result = ((bytes[position] & 0xff) << 24)
        | ((bytes[position + 1] & 0xff) << 16)
        | ((bytes[position + 2] & 0xff) << 8) | (bytes[position + 3] & 0xff);
    position += Integer.BYTES;
    return result;
  }

  public int getRowCount() { return rowCount; }
  /** FAST table hash; general loaders hash the reconstructed serialized key instead. */
  public int getHashCode() {
    if (isNull) {
      return 0;
    }
    return numericKeyWidth > 0 ? HashCodeUtil.calculateLongHashCode(longKey)
        : HashCodeUtil.murmurHash(bytes, keyOffset, keyLength);
  }
  public long getLongKey() { return longKey; }
  public boolean isNull() { return isNull; }
  public byte[] getBytes() { return bytes; }
  public int getKeyOffset() { return keyOffset; }
  public int getKeyLength() { return isNull ? 0 : keyLength; }
  public int getValueOffset() { return valueOffset; }
  public int getValueLength() { return valueLength; }
}
