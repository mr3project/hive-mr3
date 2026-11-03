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

import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.tez.ReduceRecordSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchDeserializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;

/** Expands vector shuffle frames into ordinary key/value rows for map-join hash-table loaders. */
public final class VectorMapJoinBatchReader {
  private final KeyValueReaderEdge reader;
  private final MapJoinTableContainerSerDe tableSerDe;
  private final Properties keyProperties;
  private VectorizedRowBatch batch;
  private VectorShuffleBatchDeserializer batchDeserializer;
  private VectorSerializeRow<BinarySortableSerializeWrite> keySerializer;
  private VectorSerializeRow<LazyBinarySerializeWrite> valueSerializer;
  private final Output keyOutput = new Output();
  private final Output valueOutput = new Output();
  private int nextRow;
  private BytesWritable currentKey;
  private BytesWritable currentValue;

  public VectorMapJoinBatchReader(KeyValueReaderEdge reader,
      MapJoinTableContainerSerDe tableSerDe, Properties keyProperties) {
    this.reader = reader;
    this.tableSerDe = tableSerDe;
    this.keyProperties = keyProperties;
  }

  public boolean next() throws IOException, HiveException, SerDeException {
    while (batch == null || nextRow == batch.size) {
      if (!reader.next()) {
        return false;
      }
      currentKey = reader.getCurrentKey();
      currentValue = reader.getCurrentValue();
      if (currentKey.getLength() != ReduceRecordSource.VECTOR_BATCH_KEY_BYTES.length
          || currentKey.getBytesRaw()[currentKey.getOffset()]
              != ReduceRecordSource.VECTOR_BATCH_KEY_BYTES[0]) {
        return true;
      }
      if (batch == null) {
        initializeBatch();
      }
      batchDeserializer.deserialize(currentValue, batch);
      nextRow = 0;
    }

    keySerializer.setOutput(keyOutput);
    keySerializer.serializeWrite(batch, nextRow);
    valueSerializer.setOutput(valueOutput);
    valueSerializer.serializeWrite(batch, nextRow);
    nextRow++;

    // FAST loaders enqueue byte slices for asynchronous insertion. Each expanded row owns
    // its serialized bytes, so reusing the batch and serializer outputs cannot overwrite them.
    currentKey = new BytesWritable();
    currentKey.set(keyOutput.getData(), 0, keyOutput.getLength());
    currentValue = new BytesWritable();
    currentValue.set(valueOutput.getData(), 0, valueOutput.getLength());
    return true;
  }

  public BytesWritable getCurrentKey() {
    return currentKey;
  }

  public BytesWritable getCurrentValue() {
    return currentValue;
  }

  private void initializeBatch() throws SerDeException, HiveException {
    TypeInfo[] keyTypes = VectorizedBatchUtil.typeInfosFromStructObjectInspector(
        (StructObjectInspector) tableSerDe.getKeyContext().getSerDe().getObjectInspector());
    TypeInfo[] valueTypes = VectorizedBatchUtil.typeInfosFromStructObjectInspector(
        (StructObjectInspector) tableSerDe.getValueContext().getSerDe().getObjectInspector());
    int[] keyColumns = new int[keyTypes.length];
    int[] valueColumns = new int[valueTypes.length];
    batch = new VectorizedRowBatch(keyTypes.length + valueTypes.length);
    for (int i = 0; i < keyTypes.length; i++) {
      keyColumns[i] = i;
      batch.cols[i] = VectorizedBatchUtil.createColumnVector(keyTypes[i]);
    }
    for (int i = 0; i < valueTypes.length; i++) {
      int column = keyTypes.length + i;
      valueColumns[i] = column;
      batch.cols[column] = VectorizedBatchUtil.createColumnVector(valueTypes[i]);
    }
    batchDeserializer = new VectorShuffleBatchDeserializer(batch, batch.cols.length);

    keySerializer = new VectorSerializeRow<>(
        BinarySortableSerializeWrite.with(keyProperties, keyTypes.length));
    keySerializer.init(keyTypes, keyColumns);
    valueSerializer = new VectorSerializeRow<>(new LazyBinarySerializeWrite(valueTypes.length));
    valueSerializer.init(valueTypes, valueColumns);
  }
}
