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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

final class VectorLongUniformKeyHashCodeComputer extends AbstractVectorUniformKeyHashCodeComputer {

  private final int columnNum;
  private final PrimitiveCategory primitiveCategory;

  VectorLongUniformKeyHashCodeComputer(int columnNum, PrimitiveTypeInfo primitiveTypeInfo,
      BinarySortableSerializeWrite serializeWrite) {
    super(serializeWrite);
    this.columnNum = columnNum;
    primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
  }

  @Override
  public void computeHashCodes(VectorizedRowBatch batch, int[] hashCodes) throws IOException {
    LongColumnVector longColVector = (LongColumnVector) batch.cols[columnNum];
    final int size = batch.size;
    final boolean selectedInUse = batch.selectedInUse;
    final int[] selected = batch.selected;

    if (longColVector.isRepeating) {
      final int hashCode = computeHashCode(longColVector, 0);
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = selectedInUse ? selected[logical] : logical;
        hashCodes[batchIndex] = hashCode;
      }
      return;
    }

    for (int logical = 0; logical < size; logical++) {
      final int batchIndex = selectedInUse ? selected[logical] : logical;
      hashCodes[batchIndex] = computeHashCode(longColVector, batchIndex);
    }
  }

  private int computeHashCode(LongColumnVector longColVector, int batchIndex) throws IOException {
    reset();
    if (!longColVector.noNulls && longColVector.isNull[batchIndex]) {
      serializeWrite.writeNull();
    } else {
      final long value = longColVector.vector[batchIndex];
      switch (primitiveCategory) {
      case BOOLEAN:
        serializeWrite.writeBoolean(value != 0);
        break;
      case BYTE:
        serializeWrite.writeByte((byte) value);
        break;
      case SHORT:
        serializeWrite.writeShort((short) value);
        break;
      case INT:
        serializeWrite.writeInt((int) value);
        break;
      case DATE:
        serializeWrite.writeDate((int) value);
        break;
      case LONG:
        serializeWrite.writeLong(value);
        break;
      default:
        throw new IOException("Unexpected primitive category " + primitiveCategory.name());
      }
    }
    return hash();
  }
}
