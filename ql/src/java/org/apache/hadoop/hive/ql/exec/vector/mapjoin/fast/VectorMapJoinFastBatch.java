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
 * Wire contract for single-partition FAST map-join inputs. All integers are big endian.
 * A frame starts with (version, key encoding, row count), each an int32. Each entry is
 * (signed hash32, key length, value length, key bytes, existing LazyBinary value bytes).
 * A key length of -1 denotes a scalar NULL, with no key bytes; multi-keys always retain
 * their BinarySortable bytes, including NULL markers. Numeric keys use their declared
 * signed width (booleans use canonical 0/1); strings carry their exact logical bytes.
 *
 * <p>The destination FAST table family determines the encoding. Its STRING family also
 * includes CHAR, VARCHAR and BINARY; MULTI_KEY may describe a single column. Numeric
 * hashes are calculated after the same narrowing/boolean normalization as ordinary
 * serialization. Byte-family hashes use HashCodeUtil over raw bytes for STRING and
 * BinarySortable bytes for MULTI_KEY. These are table hashes, not shuffle hashes. The
 * signed int32 is sign-extended when supplied to the existing long insertion API.</p>
 *
 * <p>Only native vector reduce sinks connected to compatible FAST loaders are marked by
 * the planner. Producers require one unordered output partition, no key tag, and no TopN
 * retention or deferred per-row bucket-number expression. After key/value expressions,
 * active rows are appended in logical order, respecting selection, repetition and NULLs.
 * Full frames are sent through the normal synchronous writer before reusing scratch
 * storage. Singleton input batches and oversized rows retain ordinary record encoding.
 * The row limit uses DEFAULT_SIZE; the byte limit is independent of the shuffle writer's
 * current buffer. The single-partition writer does not offer direct value-buffer requests.</p>
 *
 * <p>Loaders accept mixed ordinary records and frames. Entry descriptors retain immutable
 * Tez backing arrays and primitive offsets, and route using the supplied hash and the
 * receiver's own loading-thread count. Native insertion reuses the existing table cores,
 * preserving duplicates, full-outer NULL handling and numeric min/max tracking. All table
 * kinds retain uniform LazyBinary values in this protocol. Each logical row contributes
 * to progress and memory checks; physical records on marked inputs are multiplied by
 * DEFAULT_SIZE for the initial sizing/thread-count estimate. Partial frames and legacy
 * fallback records can make that heuristic overestimate cardinality.</p>
 */
public final class VectorMapJoinFastBatch {
  // Ordinary BinarySortable keys start with a NULL/non-NULL marker, not 0x03.
  // Keep this distinct from ReduceRecordSource.VECTOR_BATCH_KEY_BYTES (0x02).
  public static final byte[] KEY_BYTES = { 0x03 };
  public static final int MAX_ROWS = VectorizedRowBatch.DEFAULT_SIZE;
  public static final int MAX_BYTES = 1024 * 1024;
  static final int VERSION = 1;
  static final int HEADER_BYTES = 3 * Integer.BYTES;
  static final int ENTRY_HEADER_BYTES = 3 * Integer.BYTES;

  private VectorMapJoinFastBatch() {
  }

  public static boolean isBatchKey(BytesWritable key) {
    return key.getLength() == KEY_BYTES.length
        && key.getBytesRaw()[key.getOffset()] == KEY_BYTES[0];
  }

  // Explicit protocol IDs, independent of enum ordering.
  static int encoding(HashTableKeyType keyType) {
    switch (keyType) {
    case BOOLEAN: return 1;
    case BYTE: return 2;
    case SHORT: return 3;
    case INT: return 4;
    case DATE: return 5;
    case LONG: return 6;
    case STRING: return 7;
    case MULTI_KEY: return 8;
    default: throw new IllegalArgumentException("Unsupported FAST batch key type " + keyType);
    }
  }

  static int numericWidth(HashTableKeyType keyType) {
    switch (keyType) {
    case BOOLEAN:
    case BYTE: return 1;
    case SHORT: return 2;
    case INT:
    case DATE: return Integer.BYTES;
    case LONG: return Long.BYTES;
    default: return 0;
    }
  }
}
