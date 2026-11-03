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
package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.io.BytesWritable;

/**
 * Wire contract for FAST map-join inputs. All integers are big endian.
 * A frame starts with an int32 row count. The destination plan supplies the key type;
 * frames carry neither a version nor a key encoding ID. Numeric entries contain a
 * one-byte key flag (0 for NULL, any nonzero value for non-NULL), an int32 value length,
 * optional fixed-width key bytes, and existing LazyBinary value bytes. Producers write
 * 1 for non-NULL keys. Other entries contain int32 key and value lengths followed by
 * key bytes and LazyBinary value bytes. A key length of -1 denotes a STRING-family NULL
 * with no key bytes; multi-keys always retain BinarySortable bytes, including NULL
 * markers. Numeric keys use their declared signed width (booleans use canonical 0/1);
 * strings carry their exact logical bytes.
 *
 * <p>The destination FAST table family determines the encoding. Its STRING family also
 * includes CHAR, VARCHAR and BINARY; MULTI_KEY may describe a single column. Numeric
 * keys undergo the same narrowing/boolean normalization as ordinary serialization.
 * Receivers calculate numeric hashes from decoded keys, and byte-family hashes over
 * raw bytes for STRING and BinarySortable bytes for MULTI_KEY. Scalar NULL hashes are
 * zero. These hashes are computed once per entry at the receiver, not transmitted.
 * They are table hashes, not shuffle hashes; the signed int32 is sign-extended when
 * supplied to the existing long insertion API.</p>
 *
 * <p>Only native vector reduce sinks connected to compatible FAST loaders are marked by
 * the planner. Producers require unordered output, no key tag, and no TopN
 * retention or deferred per-row bucket-number expression. After key/value expressions,
 * active rows respect selection, repetition and NULLs. Single-partition output appends
 * rows in logical order and sends full frames through the synchronous writer before reusing
 * scratch storage. With multiple partitions, the original routing hashes group rows by
 * destination, preserving logical order within each partition. Groups above the batching
 * threshold are assembled directly into the writer's value buffer using one-entry scratch;
 * smaller groups and groups that cannot fit retain ordinary record encoding. The row limit
 * uses DEFAULT_SIZE; the writer's available slice constrains partitioned frames.
 * Single-partition frames use growing scratch storage because that writer does not offer
 * direct value-buffer requests. Neither path imposes a protocol byte limit.</p>
 *
 * <p>Loaders accept mixed ordinary records and frames. Entry descriptors retain immutable
 * Tez backing arrays and primitive offsets, and route using the computed hash and the
 * receiver's own loading-thread count. Native insertion reuses the existing table cores,
 * preserving duplicates, full-outer NULL handling and numeric min/max tracking. All table
 * kinds retain uniform LazyBinary values in this protocol. Each logical row contributes
 * to progress and memory checks. Initial sizing uses the planner's key estimate and
 * the runtime logical-record estimate without multiplying by frame size.</p>
 */
public final class VectorMapJoinFastBatch {
  // Ordinary BinarySortable keys start with a NULL/non-NULL marker, not 0x03.
  // Keep this distinct from ReduceRecordSource.VECTOR_BATCH_KEY_BYTES (0x02).
  public static final byte[] KEY_BYTES = { 0x03 };
  public static final int MAX_ROWS = VectorizedRowBatch.DEFAULT_SIZE;
  static final int HEADER_BYTES = Integer.BYTES;
  static final int NUMERIC_ENTRY_HEADER_BYTES = 1 + Integer.BYTES;
  static final int BYTES_ENTRY_HEADER_BYTES = 2 * Integer.BYTES;

  private VectorMapJoinFastBatch() {
  }

  public static boolean isBatchKey(BytesWritable key) {
    return key.getLength() == KEY_BYTES.length
        && key.getBytesRaw()[key.getOffset()] == KEY_BYTES[0];
  }

  static int numericWidth(HashTableKeyType keyType) {
    switch (keyType) {
    case BOOLEAN:
    case BYTE: return 1;
    case SHORT: return 2;
    case INT:
    case DATE: return Integer.BYTES;
    case LONG: return Long.BYTES;
    case STRING:
    case MULTI_KEY: return 0;
    default: throw new IllegalArgumentException("Unsupported FAST batch key type " + keyType);
    }
  }
}
