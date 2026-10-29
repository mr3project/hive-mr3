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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.base.Preconditions;

/** Builds bounded join-entry frames directly from evaluated vector columns. */
public final class VectorMapJoinFastBatchSerializer {
  private static final int INITIAL_BYTES = 64 * 1024;
  private final HashTableKeyType keyType;
  private final int[] keyColumnMap;
  private final BinarySortableSerializeWrite keyWriter;
  private final VectorSerializeRow<BinarySortableSerializeWrite> keyRow;
  private final LazyBinarySerializeWrite valueWriter;
  private final VectorSerializeRow<LazyBinarySerializeWrite> valueRow;
  private final BytesWritable value = new BytesWritable();
  private Output output = new Output(INITIAL_BYTES);
  private int rowCount;

  public VectorMapJoinFastBatchSerializer(HashTableKeyType keyType, TypeInfo[] keyTypes,
      int[] keyColumnMap, Properties keyProperties, TypeInfo[] valueTypes, int[] valueColumnMap)
      throws HiveException {
    VectorMapJoinFastBatch.encoding(keyType);
    Preconditions.checkArgument(keyTypes.length == keyColumnMap.length);
    Preconditions.checkArgument(valueTypes.length == valueColumnMap.length);
    Preconditions.checkArgument(keyType == HashTableKeyType.MULTI_KEY || keyColumnMap.length == 1);
    this.keyType = keyType;
    this.keyColumnMap = keyColumnMap.clone();
    if (keyType == HashTableKeyType.MULTI_KEY && keyColumnMap.length > 0) {
      keyWriter = BinarySortableSerializeWrite.with(keyProperties, keyTypes.length);
      keyRow = new VectorSerializeRow<>(keyWriter);
      keyRow.init(keyTypes, keyColumnMap);
    } else {
      keyWriter = null;
      keyRow = null;
    }
    if (valueColumnMap.length > 0) {
      valueWriter = new LazyBinarySerializeWrite(valueTypes.length);
      valueRow = new VectorSerializeRow<>(valueWriter);
      valueRow.init(valueTypes, valueColumnMap);
    } else {
      valueWriter = null;
      valueRow = null;
    }
    reset();
  }

  /**
   * Append one physical row. False leaves previously appended entries intact: flush them
   * and retry the row in a fresh frame, or use an ordinary record if that is still too large.
   * A variable-width row can transiently exceed the byte limit before its length is known.
   */
  public boolean append(VectorizedRowBatch batch, int physicalRow) throws IOException {
    if (rowCount == VectorMapJoinFastBatch.MAX_ROWS) {
      return false;
    }
    final int entryStart = output.getLength();
    output.reserve(VectorMapJoinFastBatch.ENTRY_HEADER_BYTES);
    final int keyStart = output.getLength();
    int keyLength;
    int hash;
    if (keyType == HashTableKeyType.MULTI_KEY) {
      if (keyRow != null) {
        keyWriter.setAppend(output);
        keyRow.serializeWrite(batch, physicalRow);
      }
      keyLength = output.getLength() - keyStart;
      hash = HashCodeUtil.murmurHash(output.getData(), keyStart, keyLength);
    } else {
      final ColumnVector column = batch.cols[keyColumnMap[0]];
      final int row = column.isRepeating ? 0 : physicalRow;
      if (!column.noNulls && column.isNull[row]) {
        keyLength = -1;
        hash = 0;
      } else if (keyType == HashTableKeyType.STRING) {
        // This family includes CHAR, VARCHAR and BINARY. Expressions already provide
        // their logical bytes; do not introduce text conversion or padding here.
        final BytesColumnVector bytes = (BytesColumnVector) column;
        keyLength = bytes.length[row];
        hash = HashCodeUtil.murmurHash(bytes.vector[row], bytes.start[row], keyLength);
        output.write(bytes.vector[row], bytes.start[row], keyLength);
      } else {
        final long key = normalize(((LongColumnVector) column).vector[row]);
        keyLength = VectorMapJoinFastBatch.numericWidth(keyType);
        hash = HashCodeUtil.calculateLongHashCode(key);
        for (int shift = (keyLength - 1) * Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
          output.write((int) (key >>> shift) & 0xff);
        }
      }
    }
    final int valueStart = output.getLength();
    if (valueRow != null) {
      valueWriter.setAppend(output);
      valueRow.serializeWrite(batch, physicalRow);
    }
    final int valueLength = output.getLength() - valueStart;
    if (output.getLength() > VectorMapJoinFastBatch.MAX_BYTES) {
      output.setWritePosition(entryStart);
      return false;
    }
    output.writeInt(entryStart, hash);
    output.writeInt(entryStart + Integer.BYTES, keyLength);
    output.writeInt(entryStart + 2 * Integer.BYTES, valueLength);
    rowCount++;
    return true;
  }

  private long normalize(long key) {
    switch (keyType) {
    case BOOLEAN: return key == 0 ? 0 : 1;
    case BYTE: return (byte) key;
    case SHORT: return (short) key;
    case INT:
    case DATE: return (int) key;
    case LONG: return key;
    default: throw new IllegalStateException("Not a numeric key type " + keyType);
    }
  }

  public int getRowCount() {
    return rowCount;
  }

  /** Borrowed until reset/append; the current single-partition writer consumes it synchronously. */
  public BytesWritable getValue() {
    Preconditions.checkState(rowCount > 0);
    output.writeInt(2 * Integer.BYTES, rowCount);
    value.setDirect(output.getData(), 0, output.getLength());
    return value;
  }

  public void reset() {
    // Do not retain a huge scratch array after an oversized row takes the legacy path.
    if (output.getData().length > 2 * VectorMapJoinFastBatch.MAX_BYTES) {
      output = new Output(INITIAL_BYTES);
    } else {
      output.reset();
    }
    // Release the wrapper's borrowed reference before reusing/replacing the scratch array.
    value.setDirect(output.getData(), 0, 0);
    rowCount = 0;
    output.appendInt(VectorMapJoinFastBatch.VERSION);
    output.appendInt(VectorMapJoinFastBatch.encoding(keyType));
    output.appendInt(0);
  }
}
