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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

/**
 * Wire contract for single-partition map-join inputs. All integers are big endian.
 * A frame starts with an int32 row count. Each endpoint derives the key type from its schema;
 * frames carry neither a version nor a key encoding ID. Numeric entries contain a
 * one-byte key flag (0 for NULL, any nonzero value for non-NULL), an int32 value length,
 * optional fixed-width key bytes, and existing LazyBinary value bytes. Producers write
 * 1 for non-NULL keys. Other entries contain int32 key and value lengths followed by
 * key bytes and LazyBinary value bytes. A key length of -1 denotes a STRING-family NULL
 * with no key bytes; multi-keys always retain BinarySortable bytes, including NULL
 * markers. Numeric keys use their declared signed width (booleans use canonical 0/1);
 * strings carry their exact logical bytes.
 *
 * <p>Encoding follows the native FAST key-family selection, including the multikey-only
 * setting. The STRING family includes CHAR, VARCHAR and BINARY; MULTI_KEY may describe
 * a single column. Numeric
 * keys undergo the same narrowing/boolean normalization as ordinary serialization.
 * FAST receivers calculate numeric hashes from decoded keys, and byte-family hashes over
 * raw bytes for STRING and BinarySortable bytes for MULTI_KEY. Scalar NULL hashes are
 * zero. These hashes are computed once per entry at the receiver, not transmitted.
 * They are table hashes, not shuffle hashes; the signed int32 is sign-extended when
 * supplied to the existing long insertion API. General loaders reconstruct ordinary
 * BinarySortable keys and use their containers' existing hashing and insertion paths.</p>
 *
 * <p>Reduce sinks are marked as map-join outputs when small-table edges are connected.
 * Native vector producers require one unordered output partition, no key tag, and no TopN
 * retention or deferred per-row bucket-number expression. After key/value expressions,
 * active rows are appended in logical order, respecting selection, repetition and NULLs.
 * Full frames are sent through the normal synchronous writer before reusing scratch
 * storage. Singleton input batches and oversized rows retain ordinary record encoding.
 * The row limit uses DEFAULT_SIZE; the byte limit is independent of the shuffle writer's
 * current buffer. The single-partition writer does not offer direct value-buffer requests.</p>
 *
 * <p>Loaders accept mixed ordinary records and frames. FAST entry descriptors retain
 * immutable Tez backing arrays and primitive offsets, and route using the computed hash
 * and the receiver's own loading-thread count. Native insertion reuses the existing table cores,
 * preserving duplicates, full-outer NULL handling and numeric min/max tracking. All table
 * kinds retain uniform LazyBinary values in this protocol. Each logical row contributes
 * to progress and memory checks. Initial sizing uses the planner's key estimate and
 * the runtime logical-record estimate without multiplying by frame size.</p>
 */
public final class VectorMapJoinBatch {
  // Ordinary BinarySortable keys start with a NULL/non-NULL marker, not 0x03.
  // Keep this distinct from ReduceRecordSource.VECTOR_BATCH_KEY_BYTES (0x02).
  public static final byte[] KEY_BYTES = { 0x03 };
  public static final int MAX_ROWS = VectorizedRowBatch.DEFAULT_SIZE;
  public static final int MAX_BYTES = 1024 * 1024;
  static final int HEADER_BYTES = Integer.BYTES;
  static final int NUMERIC_ENTRY_HEADER_BYTES = 1 + Integer.BYTES;
  static final int BYTES_ENTRY_HEADER_BYTES = 2 * Integer.BYTES;

  private VectorMapJoinBatch() {
  }

  public static boolean isBatchKey(BytesWritable key) {
    return key.getLength() == KEY_BYTES.length
        && key.getBytesRaw()[key.getOffset()] == KEY_BYTES[0];
  }

  /** Shared wire encoding and native table family selection from the declared join keys. */
  public static HashTableKeyType getKeyType(List<ExprNodeDesc> keys, boolean multiKeyOnly) {
    if (multiKeyOnly || keys.size() != 1) {
      return HashTableKeyType.MULTI_KEY;
    }
    TypeInfo typeInfo = keys.get(0).getTypeInfo();
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      return HashTableKeyType.MULTI_KEY;
    }
    switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
    case BOOLEAN: return HashTableKeyType.BOOLEAN;
    case BYTE: return HashTableKeyType.BYTE;
    case SHORT: return HashTableKeyType.SHORT;
    case INT: return HashTableKeyType.INT;
    case DATE: return HashTableKeyType.DATE;
    case LONG: return HashTableKeyType.LONG;
    case STRING:
    case CHAR:
    case VARCHAR:
    case BINARY: return HashTableKeyType.STRING;
    default: return HashTableKeyType.MULTI_KEY;
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
    case STRING:
    case MULTI_KEY: return 0;
    default: throw new IllegalArgumentException("Unsupported map-join batch key type " + keyType);
    }
  }
}
