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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

final class VectorMultiUniformKeyHashCodeComputer extends AbstractVectorUniformKeyHashCodeComputer {

  private final VectorSerializeRow<BinarySortableSerializeWrite> keySerializeRow;

  VectorMultiUniformKeyHashCodeComputer(TypeInfo[] typeInfos, int[] columnNums,
      BinarySortableSerializeWrite serializeWrite) throws HiveException {
    super(serializeWrite);
    keySerializeRow = new VectorSerializeRow<>(serializeWrite);
    keySerializeRow.init(typeInfos, columnNums);
  }

  @Override
  public void computeHashCodes(VectorizedRowBatch batch, int[] hashCodes) throws IOException {
    final int size = batch.size;
    final boolean selectedInUse = batch.selectedInUse;
    final int[] selected = batch.selected;

    for (int logical = 0; logical < size; logical++) {
      final int batchIndex = selectedInUse ? selected[logical] : logical;
      keySerializeRow.setOutput(output);
      keySerializeRow.serializeWrite(batch, batchIndex);
      hashCodes[batchIndex] = hash();
    }
  }
}
