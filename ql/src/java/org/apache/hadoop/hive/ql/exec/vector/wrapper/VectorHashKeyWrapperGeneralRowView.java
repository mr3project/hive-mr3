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
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * A fixed row identity over reusable batch storage. Copies own their values and string bytes.
 */
public final class VectorHashKeyWrapperGeneralRowView extends VectorHashKeyWrapperBase {

  final VectorHashKeyWrapperGeneralBatchStorage owner;
  final int row;

  VectorHashKeyWrapperGeneralRowView(VectorHashKeyWrapperGeneralBatchStorage owner, int row) {
    this.owner = owner;
    this.row = row;
  }

  @Override
  public void setHashKey() {
    hashcode = owner.computeHash(row);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperGeneralRowView) {
      VectorHashKeyWrapperGeneralRowView keyThat = (VectorHashKeyWrapperGeneralRowView) that;
      return owner.equalRows(row, keyThat.owner, keyThat.row);
    }
    if (that instanceof VectorHashKeyWrapperGeneral) {
      return ((VectorHashKeyWrapperGeneral) that).equalsRow(owner, row);
    }
    return false;
  }

  @Override
  protected Object clone() {
    ++owner.cloneCalls;
    return VectorHashKeyWrapperGeneral.fromRow(owner, row);
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    ++owner.copyKeyCalls;
    assert oldWrapper instanceof VectorHashKeyWrapperGeneral;
    ((VectorHashKeyWrapperGeneral) oldWrapper).copyFromRow(owner, row);
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
    assert bytes != null;
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
    // owner.byteStarts[offset] = 0;    // optional: null strings are skipped before their offsets are read
    owner.byteLengths[offset] = -1;
  }

  @Override
  public void assignDouble(int index, double value) {
    owner.doubleValues[index * owner.capacity + row] = value;
  }

  @Override
  public void assignNullDouble(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    owner.doubleValues[index * owner.capacity + row] = 0;
  }

  @Override
  public void assignDecimal(int index, HiveDecimalWritable value) {
    owner.decimalValues[index * owner.capacity + row].set(value);
  }

  @Override
  public void assignNullDecimal(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    owner.decimalValues[index * owner.capacity + row].set(HiveDecimal.ZERO);
  }

  @Override
  public void assignTimestamp(int index, Timestamp value) {
    Timestamp destination = owner.timestampValues[index * owner.capacity + row];
    destination.setTime(value.getTime());
    destination.setNanos(value.getNanos());
  }

  @Override
  public void assignTimestamp(int index, TimestampColumnVector columnVector, int elementNum) {
    columnVector.timestampUpdate(owner.timestampValues[index * owner.capacity + row], elementNum);
  }

  @Override
  public void assignNullTimestamp(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    Timestamp destination = owner.timestampValues[index * owner.capacity + row];
    destination.setTime(0);
    destination.setNanos(0);
  }

  @Override
  public void assignIntervalDayTime(int index, HiveIntervalDayTime value) {
    owner.intervalDayTimeValues[index * owner.capacity + row].set(value);
  }

  @Override
  public void assignIntervalDayTime(int index, IntervalDayTimeColumnVector columnVector, int elementNum) {
    owner.intervalDayTimeValues[index * owner.capacity + row].set(
        columnVector.asScratchIntervalDayTime(elementNum));
  }

  @Override
  public void assignNullIntervalDayTime(int keyIndex, int index) {
    owner.isNull[keyIndex * owner.capacity + row] = true;
    owner.intervalDayTimeValues[index * owner.capacity + row].set(0, 0);
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
  public double getDoubleValue(int i) {
    return owner.doubleValues[i * owner.capacity + row];
  }

  @Override
  public HiveDecimalWritable getDecimal(int i) {
    return owner.decimalValues[i * owner.capacity + row];
  }

  @Override
  public Timestamp getTimestamp(int i) {
    return owner.timestampValues[i * owner.capacity + row];
  }

  @Override
  public HiveIntervalDayTime getIntervalDayTime(int i) {
    return owner.intervalDayTimeValues[i * owner.capacity + row];
  }

  @Override
  public boolean isNull(int keyIndex) {
    return owner.isNull[keyIndex * owner.capacity + row];
  }

  @Override
  public void clearIsNull() {
    ++owner.clearRowIsNullCalls;
    for (int i = 0; i < owner.keyCount; ++i) {
      owner.isNull[i * owner.capacity + row] = false;
    }
  }

  @Override
  public void setNull() {
    ++owner.setNullCalls;
    for (int i = 0; i < owner.keyCount; ++i) {
      owner.isNull[i * owner.capacity + row] = true;
    }
  }

  @Override
  public int getVariableSize() {
    ++owner.getVariableSizeCalls;
    int variableSize = 0;
    JavaDataModel model = JavaDataModel.get();
    for (int i = 0; i < owner.stringCount; ++i) {
      variableSize += model.lengthForByteArrayOfSize(getByteLength(i));
    }
    return variableSize;
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo) {
    StringBuilder sb = new StringBuilder();
    boolean isFirstKey = true;

    if (owner.longCount > 0) {
      isFirstKey = false;
      sb.append("longs ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.longIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.longIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getLongValue(i));
          PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
          // FUTURE: Add INTERVAL_YEAR_MONTH, etc, as desired.
          switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case DATE:
            {
              Date dt = new Date(0);
              dt.setTime(DateWritableV2.daysToMillis((int) getLongValue(i)));
              sb.append(" date ");
              sb.append(dt.toString());
            }
            break;
          default:
            // Add nothing more.
            break;
          }
        }
      }
    }
    if (owner.doubleCount > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("doubles ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.doubleIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.doubleIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getDoubleValue(i));
        }
      }
    }
    if (owner.stringCount > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.stringIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
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
    if (owner.decimalCount > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("decimals ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.decimalIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.decimalIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getDecimal(i));
        }
      }
    }
    if (owner.timestampCount > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("timestamps ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.timestampIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.timestampIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getTimestamp(i));
        }
      }
    }
    if (owner.intervalDayTimeCount > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("interval day times ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.intervalDayTimeIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.intervalDayTimeIndices[i];
        if (isNull(keyIndex)) {
          sb.append("null");
        } else {
          sb.append(getIntervalDayTime(i));
        }
      }
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (owner.longCount > 0) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("longs [");
      for (int i = 0; i < owner.longCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getLongValue(i));
      }
      sb.append(']');
    }
    if (owner.doubleCount > 0) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("doubles [");
      for (int i = 0; i < owner.doubleCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getDoubleValue(i));
      }
      sb.append(']');
    }
    if (owner.stringCount > 0) {
      if (sb.length() > 0) {
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
    if (owner.decimalCount > 0) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("decimals [");
      for (int i = 0; i < owner.decimalCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getDecimal(i));
      }
      sb.append(']');
    }
    if (owner.timestampCount > 0) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("timestamps [");
      for (int i = 0; i < owner.timestampCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getTimestamp(i));
      }
      sb.append(']');
    }
    if (owner.intervalDayTimeCount > 0) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("interval day times [");
      for (int i = 0; i < owner.intervalDayTimeCount; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getIntervalDayTime(i));
      }
      sb.append(']');
    }
    if (sb.length() > 0) {
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
