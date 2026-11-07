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

import java.sql.Date;

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * A fixed row identity over reusable batch storage. Copies own their values and string bytes.
 */
public final class VectorHashKeyWrapperLongStringRowView extends VectorHashKeyWrapperBase {

  final VectorHashKeyWrapperLongStringBatchStorage owner;
  final int row;

  VectorHashKeyWrapperLongStringRowView(VectorHashKeyWrapperLongStringBatchStorage owner, int row) {
    this.owner = owner;
    this.row = row;
  }

  @Override
  public void setHashKey() {
    hashcode = owner.computeHash(row);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperLongStringRowView) {
      VectorHashKeyWrapperLongStringRowView keyThat = (VectorHashKeyWrapperLongStringRowView) that;
      return owner.equalRows(row, keyThat.owner, keyThat.row);
    }
    if (that instanceof VectorHashKeyWrapperGeneralLongString) {
      return ((VectorHashKeyWrapperGeneralLongString) that).equalsRow(owner, row);
    }
    return false;
  }

  @Override
  protected Object clone() {
    return VectorHashKeyWrapperGeneralLongString.fromRow(owner, row);
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    ((VectorHashKeyWrapperGeneralLongString) oldWrapper).copyFromRow(owner, row);
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    owner.isNull[keyIndex * owner.capacity + row] = false;
    owner.longValues[index * owner.capacity + row] = v;
  }

  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    owner.longValues[index * owner.capacity + row] = v;
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    owner.longValues[index * owner.capacity + row] = 0;
  }

  @Override
  public void assignString(int index, byte[] bytes, int start, int length) {
    int offset = index * owner.capacity + row;
    owner.byteValues[offset] = bytes;
    owner.byteStarts[offset] = start;
    owner.byteLengths[offset] = length;
  }

  @Override
  public void assignNullString(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    int offset = index * owner.capacity + row;
    owner.byteValues[offset] = null;
    owner.byteStarts[offset] = 0;
    owner.byteLengths[offset] = -1;
  }

  @Override
  public long getLongValue(int i) {
    return owner.longValues[i * owner.capacity + row];
  }

  @Override
  public byte[] getBytes(int i) {
    return owner.byteValues[i * owner.capacity + row];
  }

  @Override
  public int getByteStart(int i) {
    return owner.byteStarts[i * owner.capacity + row];
  }

  @Override
  public int getByteLength(int i) {
    return owner.byteLengths[i * owner.capacity + row];
  }

  @Override
  public boolean isNull(int keyIndex) {
    return owner.isNull[keyIndex * owner.capacity + row];
  }

  @Override
  public void clearIsNull() {
    for (int i = 0; i < owner.keyCount; ++i) {
      owner.isNull[i * owner.capacity + row] = false;
    }
  }

  @Override
  public void setNull() {
    for (int i = 0; i < owner.keyCount; ++i) {
      owner.isNull[i * owner.capacity + row] = true;
    }
  }

  @Override
  public int getVariableSize() {
    int variableSize = 0;
    JavaDataModel model = JavaDataModel.get();
    for (int i = 0; i < owner.stringCount; ++i) {
      variableSize += model.lengthForByteArrayOfSize(getByteLength(i));
    }
    return variableSize;
  }

  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo) {
    StringBuilder sb = new StringBuilder();
    if (owner.longCount > 0) {
      sb.append("longs ");
      for (int i = 0; i < columnSetInfo.longIndices.length; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.longIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          long value = getLongValue(i);
          sb.append(value);
          PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
          switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case DATE:
            Date date = new Date(0);
            date.setTime(DateWritableV2.daysToMillis((int) value));
            sb.append(" date ");
            sb.append(date.toString());
            break;
          default:
            break;
          }
        }
      }
    }
    if (owner.stringCount > 0) {
      if (owner.longCount > 0) {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      for (int i = 0; i < columnSetInfo.stringIndices.length; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.stringIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getByteLength(i));
        }
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (owner.longCount > 0) {
      sb.append("longs [");
      for (int i = 0; i < owner.longCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getLongValue(i));
      }
      sb.append(']');
    }
    if (owner.stringCount > 0) {
      if (owner.longCount > 0) {
        sb.append(", ");
      }
      sb.append("byte lengths [");
      for (int i = 0; i < owner.stringCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getByteLength(i));
      }
      sb.append(']');
    }
    if (owner.longCount > 0 || owner.stringCount > 0) {
      sb.append(", ");
    }
    sb.append("nulls [");
    for (int i = 0; i < owner.keyCount; ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(isNull(i));
    }
    sb.append(']');
    return sb.toString();
  }
}
