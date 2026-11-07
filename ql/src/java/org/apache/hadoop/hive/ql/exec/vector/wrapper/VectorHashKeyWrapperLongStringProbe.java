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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hive.common.util.Murmur3;

/**
 * A mutable hash-aggregation lookup key over input columns for one grouping-set pass.
 * Selecting a row changes its identity. Only independent copies may be retained.
 */
public final class VectorHashKeyWrapperLongStringProbe extends VectorHashKeyWrapperBase {

  final VectorColumnSetInfo columnSetInfo;
  final HashContext hashCtx;

  private final int[] outputColumns;
  private final ColumnVector[] columns;
  private final LongColumnVector[] longColumns;
  private final BytesColumnVector[] stringColumns;
  private final boolean[] overrideIsNull;

  private VectorizedRowBatch batch;
  private int row;

  public VectorHashKeyWrapperLongStringProbe(VectorColumnSetInfo columnSetInfo,
      VectorExpression[] keyExpressions, HashContext hashCtx) {
    this.columnSetInfo = columnSetInfo;
    this.hashCtx = hashCtx;
    outputColumns = new int[columnSetInfo.keyCount];
    columns = new ColumnVector[columnSetInfo.keyCount];
    longColumns = new LongColumnVector[columnSetInfo.longIndices.length];
    stringColumns = new BytesColumnVector[columnSetInfo.stringIndices.length];
    overrideIsNull = new boolean[columnSetInfo.keyCount];
    for (int keyIndex = 0; keyIndex < outputColumns.length; ++keyIndex) {
      outputColumns[keyIndex] = keyExpressions[keyIndex].getOutputColumnNum();
    }
    assert longColumns.length + stringColumns.length == columnSetInfo.keyCount;
  }

  public void bind(VectorizedRowBatch batch, boolean[] groupingOverrides) {
    this.batch = batch;
    for (int keyIndex = 0; keyIndex < columns.length; ++keyIndex) {
      columns[keyIndex] = batch.cols[outputColumns[keyIndex]];
      overrideIsNull[keyIndex] = groupingOverrides != null && groupingOverrides[keyIndex];
    }
    for (int i = 0; i < longColumns.length; ++i) {
      longColumns[i] = (LongColumnVector) columns[columnSetInfo.longIndices[i]];
    }
    for (int i = 0; i < stringColumns.length; ++i) {
      stringColumns[i] = (BytesColumnVector) columns[columnSetInfo.stringIndices[i]];
    }
  }

  public void selectRow(int denseRow) {
    row = batch.selectedInUse ? batch.selected[denseRow] : denseRow;
    setHashKey();
  }

  @Override
  public void setHashKey() {
    int longHash = 1;
    for (int i = 0; i < longColumns.length; ++i) {
      long value = getLongValue(i);
      longHash = 31 * longHash + (int) (value ^ (value >>> 32));
    }
    int nullHash = 1;
    for (int keyIndex = 0; keyIndex < columns.length; ++keyIndex) {
      nullHash = 31 * nullHash + (isNull(keyIndex) ? 1231 : 1237);
    }
    int hash = longHash ^ nullHash;

    Murmur3.IncrementalHash32 bytesHash = null;
    byte[] pendingBytes = null;
    int pendingStart = 0;
    int pendingLength = 0;
    boolean hasNonNull = false;
    for (int i = 0; i < stringColumns.length; ++i) {
      if (isNull(columnSetInfo.stringIndices[i])) {
        continue;
      }
      BytesColumnVector column = stringColumns[i];
      int inputRow = column.isRepeating ? 0 : row;
      int length = column.length[inputRow];
      hasNonNull = true;
      if (length > 0) {
        if (pendingLength > 0) {
          if (bytesHash == null) {
            bytesHash = HashContext.getBytesHash(hashCtx);
            bytesHash.start(hash);
          }
          bytesHash.add(pendingBytes, pendingStart, pendingLength);
        }
        pendingBytes = column.vector[inputRow];
        pendingStart = column.start[inputRow];
        pendingLength = length;
      }
    }
    if (bytesHash != null) {
      hash = bytesHash.addAndEnd(pendingBytes, pendingStart, pendingLength);
    } else if (pendingLength > 0) {
      hash = Murmur3.IncrementalHash32.hash32(pendingBytes, pendingStart, pendingLength, hash);
    } else if (hasNonNull) {
      bytesHash = HashContext.getBytesHash(hashCtx);
      bytesHash.start(hash);
      hash = bytesHash.end();
    }
    hashcode = hash;
  }

  @Override
  public boolean equals(Object that) {
    if (that == this) {
      return true;
    }
    if (that instanceof VectorHashKeyWrapperGeneralLongString) {
      return ((VectorHashKeyWrapperGeneralLongString) that).equalsProbe(this);
    }
    if (that instanceof VectorHashKeyWrapperLongStringRowView) {
      return equalsRow((VectorHashKeyWrapperLongStringRowView) that);
    }
    if (that instanceof VectorHashKeyWrapperLongStringProbe) {
      VectorHashKeyWrapperLongStringProbe probe = (VectorHashKeyWrapperLongStringProbe) that;
      return hashcode == probe.hashcode &&
          longColumns.length == probe.longColumns.length &&
          stringColumns.length == probe.stringColumns.length &&
          columns.length == probe.columns.length && equalValues(probe);
    }
    return false;
  }

  boolean equalsRow(VectorHashKeyWrapperLongStringRowView rowView) {
    return hashcode == rowView.hashcode &&
        longColumns.length == rowView.owner.longCount &&
        stringColumns.length == rowView.owner.stringCount &&
        columns.length == rowView.owner.keyCount && equalValues(rowView);
  }

  private boolean equalValues(VectorHashKeyWrapperBase other) {
    for (int i = 0; i < longColumns.length; ++i) {
      if (getLongValue(i) != other.getLongValue(i)) {
        return false;
      }
    }
    for (int keyIndex = 0; keyIndex < columns.length; ++keyIndex) {
      if (isNull(keyIndex) != other.isNull(keyIndex)) {
        return false;
      }
    }
    for (int i = 0; i < stringColumns.length; ++i) {
      if (!isNull(columnSetInfo.stringIndices[i]) &&
          !equalBytes(i, other.getBytes(i), other.getByteStart(i), other.getByteLength(i))) {
        return false;
      }
    }
    return true;
  }

  boolean equalBytes(int index, byte[] bytes, int start, int length) {
    BytesColumnVector column = stringColumns[index];
    int inputRow = column.isRepeating ? 0 : row;
    return StringExpr.equal(bytes, start, length,
        column.vector[inputRow], column.start[inputRow], column.length[inputRow]);
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperGeneralLongString copy = new VectorHashKeyWrapperGeneralLongString(
        hashCtx, longColumns.length, stringColumns.length, columns.length);
    copy.copyFromProbe(this);
    return copy;
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    ((VectorHashKeyWrapperGeneralLongString) oldWrapper).copyFromProbe(this);
  }

  @Override
  public boolean isNull(int keyIndex) {
    ColumnVector column = columns[keyIndex];
    return overrideIsNull[keyIndex] ||
        (!column.noNulls && column.isNull[column.isRepeating ? 0 : row]);
  }

  @Override
  public long getLongValue(int index) {
    LongColumnVector column = longColumns[index];
    return isNull(columnSetInfo.longIndices[index]) ? 0 :
        column.vector[column.isRepeating ? 0 : row];
  }

  @Override
  public byte[] getBytes(int index) {
    BytesColumnVector column = stringColumns[index];
    return isNull(columnSetInfo.stringIndices[index]) ? null :
        column.vector[column.isRepeating ? 0 : row];
  }

  @Override
  public int getByteStart(int index) {
    BytesColumnVector column = stringColumns[index];
    return isNull(columnSetInfo.stringIndices[index]) ? 0 :
        column.start[column.isRepeating ? 0 : row];
  }

  @Override
  public int getByteLength(int index) {
    BytesColumnVector column = stringColumns[index];
    return isNull(columnSetInfo.stringIndices[index]) ? -1 :
        column.length[column.isRepeating ? 0 : row];
  }

  public int getVariableSize(int batchSize) {
    int variableSize = 0;
    JavaDataModel model = JavaDataModel.get();
    for (int i = 0; i < stringColumns.length; ++i) {
      BytesColumnVector column = stringColumns[i];
      if (overrideIsNull[columnSetInfo.stringIndices[i]]) {
        variableSize += batchSize * model.lengthForByteArrayOfSize(-1);
      } else if (column.isRepeating) {
        int length = !column.noNulls && column.isNull[0] ? -1 : column.length[0];
        variableSize += batchSize * model.lengthForByteArrayOfSize(length);
      } else {
        for (int denseRow = 0; denseRow < batchSize; ++denseRow) {
          int inputRow = batch.selectedInUse ? batch.selected[denseRow] : denseRow;
          int length = !column.noNulls && column.isNull[inputRow] ? -1 : column.length[inputRow];
          variableSize += model.lengthForByteArrayOfSize(length);
        }
      }
    }
    return variableSize;
  }
}
