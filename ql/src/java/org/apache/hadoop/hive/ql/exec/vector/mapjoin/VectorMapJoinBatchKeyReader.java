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
import java.util.Properties;

import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.io.BytesWritable;

/** Restores ordinary BinarySortable keys for map-join containers that load serialized records. */
public final class VectorMapJoinBatchKeyReader {
  private final HashTableKeyType keyType;
  private final BinarySortableSerializeWrite keyWriter;
  private final Output output;
  private final BytesWritable key = new BytesWritable();

  public VectorMapJoinBatchKeyReader(HashTableKeyType keyType, Properties keyProperties) {
    this.keyType = keyType;
    keyWriter = keyType == HashTableKeyType.MULTI_KEY
        ? null : BinarySortableSerializeWrite.with(keyProperties, 1);
    output = keyWriter == null ? null : new Output();
  }

  /** The returned wrapper and scalar scratch bytes are borrowed until the next call. */
  public BytesWritable getKey(VectorMapJoinBatchDeserializer reader) throws IOException {
    if (keyType == HashTableKeyType.MULTI_KEY) {
      key.setDirect(reader.getBytes(), reader.getKeyOffset(), reader.getKeyLength());
      return key;
    }
    keyWriter.set(output);
    if (reader.isNull()) {
      keyWriter.writeNull();
    } else {
      switch (keyType) {
      case BOOLEAN:
        keyWriter.writeBoolean(reader.getLongKey() != 0);
        break;
      case BYTE:
        keyWriter.writeByte((byte) reader.getLongKey());
        break;
      case SHORT:
        keyWriter.writeShort((short) reader.getLongKey());
        break;
      case INT:
        keyWriter.writeInt((int) reader.getLongKey());
        break;
      case DATE:
        keyWriter.writeDate((int) reader.getLongKey());
        break;
      case LONG:
        keyWriter.writeLong(reader.getLongKey());
        break;
      case STRING:
        // STRING, CHAR, VARCHAR and BINARY use the same BinarySortable byte encoding.
        keyWriter.writeString(reader.getBytes(), reader.getKeyOffset(), reader.getKeyLength());
        break;
      default:
        throw new IllegalStateException("Unsupported scalar map-join batch key type " + keyType);
      }
    }
    key.setDirect(output.getData(), 0, output.getLength());
    return key;
  }
}
