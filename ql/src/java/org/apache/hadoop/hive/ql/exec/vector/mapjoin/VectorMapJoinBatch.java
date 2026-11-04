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
 * Frames for single-partition map-join inputs.
 * Integers are big endian.
 * Endpoints derive the key family from their schemas and the multikey-only setting.
 * No hashes are transmitted.
 *
 * <pre>
 * Record key:   [0x03]
 * Record value: [rowCount:i32][entry 0] ... [entry rowCount-1]
 *
 * Entry layouts (value bytes are always LazyBinary):
 * Numeric:      [1:u8][valueLength:i32][key:W bytes][value bytes]
 * Numeric NULL: [0:u8][valueLength:i32]             [value bytes]
 * STRING:       [keyLength:i32][valueLength:i32][raw key bytes][value bytes]
 * STRING NULL:  [-1:i32]       [valueLength:i32]               [value bytes]
 * MULTI_KEY:    [keyLength:i32][valueLength:i32][BinarySortable key][value bytes]
 * </pre>
 *
 * <p>Numeric width W is 1 for BOOLEAN/BYTE, 2 for SHORT, 4 for INT/DATE, and 8 for LONG.
 * Numeric keys use their declared signed width.
 * BOOLEAN values are normalized to 0/1.
 * Producers write flag 1 for non-NULL.
 * Readers accept any nonzero flag.
 * STRING includes CHAR, VARCHAR and BINARY.
 * MULTI_KEY retains embedded NULL markers and may encode one column.</p>
 *
 * <p>Frames contain at most {@link #MAX_ROWS} entries.
 * The serializer grows and reuses its backing buffer.
 * Loaders also accept ordinary records for singleton input batches.
 * FAST loaders insert native keys directly.
 * General loaders restore BinarySortable keys.</p>
 */
public final class VectorMapJoinBatch {
  // Ordinary BinarySortable keys start with a NULL/non-NULL marker, not 0x03.
  // Keep this distinct from ReduceRecordSource.VECTOR_BATCH_KEY_BYTES (0x02).
  public static final byte[] KEY_BYTES = { 0x03 };
  public static final int MAX_ROWS = VectorizedRowBatch.DEFAULT_SIZE;
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
