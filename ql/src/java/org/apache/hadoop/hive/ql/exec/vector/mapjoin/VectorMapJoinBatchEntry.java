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

/**
 * Immutable descriptor for a queued map-join entry.
 * Backing arrays remain shared and must stay immutable until insertion completes.
 */
public final class VectorMapJoinBatchEntry {
  public final long hashCode;
  public final boolean nativeKey;
  public final boolean isNull;
  public final long longKey;
  public final byte[] keyBytes;
  public final int keyOffset, keyLength;
  public final byte[] valueBytes;
  public final int valueOffset, valueLength;

  public VectorMapJoinBatchEntry(long hashCode, byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength) {
    this(hashCode, false, false, 0, keyBytes, keyOffset, keyLength,
        valueBytes, valueOffset, valueLength);
  }

  VectorMapJoinBatchEntry(long hashCode, boolean nativeKey, boolean isNull, long longKey,
      byte[] keyBytes, int keyOffset, int keyLength,
      byte[] valueBytes, int valueOffset, int valueLength) {
    this.hashCode = hashCode;
    this.nativeKey = nativeKey;
    this.isNull = isNull;
    this.longKey = longKey;
    this.keyBytes = keyBytes;
    this.keyOffset = keyOffset;
    this.keyLength = keyLength;
    this.valueBytes = valueBytes;
    this.valueOffset = valueOffset;
    this.valueLength = valueLength;
  }
}
