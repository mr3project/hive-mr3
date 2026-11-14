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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/** Serializes evaluated runtime-value fields using the native vector ReduceSink value codec. */
final class RuntimeValueSerializer {
  private interface FieldWriter {
    void write(Object value) throws IOException;
  }

  private final LazyBinarySerializeWrite serializeWrite;
  private final FieldWriter[] fieldWriters;
  private final Output output = new Output();
  private final BytesWritable serializedValue = new BytesWritable();

  RuntimeValueSerializer(StructObjectInspector inspector) {
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    serializeWrite = new LazyBinarySerializeWrite(fields.size());
    serializeWrite.set(output);
    fieldWriters = new FieldWriter[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      assert fields.get(i).getFieldObjectInspector().getCategory() == Category.PRIMITIVE;
      fieldWriters[i] = createFieldWriter((PrimitiveObjectInspector) fields.get(i).getFieldObjectInspector());
    }
  }

  BytesWritable serialize(Object[] values) throws IOException {
    assert values.length == fieldWriters.length;
    serializeWrite.reset();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        serializeWrite.writeNull();
      } else {
        fieldWriters[i].write(values[i]);
      }
    }
    serializedValue.set(output.getData(), 0, output.getLength());
    return serializedValue;
  }

  private FieldWriter createFieldWriter(PrimitiveObjectInspector inspector) {
    switch (inspector.getPrimitiveCategory()) {
    case VOID:
      return value -> serializeWrite.writeNull();
    case BOOLEAN:
      return value -> serializeWrite.writeBoolean(((BooleanObjectInspector) inspector).get(value));
    case BYTE:
      return value -> serializeWrite.writeByte(((ByteObjectInspector) inspector).get(value));
    case SHORT:
      return value -> serializeWrite.writeShort(((ShortObjectInspector) inspector).get(value));
    case INT:
      return value -> serializeWrite.writeInt(((IntObjectInspector) inspector).get(value));
    case LONG:
      return value -> serializeWrite.writeLong(((LongObjectInspector) inspector).get(value));
    case FLOAT:
      return value -> serializeWrite.writeFloat(((FloatObjectInspector) inspector).get(value));
    case DOUBLE:
      return value -> serializeWrite.writeDouble(((DoubleObjectInspector) inspector).get(value));
    case STRING:
      return value -> writeText(((StringObjectInspector) inspector).getPrimitiveWritableObject(value));
    case CHAR:
      return value -> writeText(((HiveCharObjectInspector) inspector)
          .getPrimitiveWritableObject(value).getStrippedValue());
    case VARCHAR:
      return value -> writeText(((HiveVarcharObjectInspector) inspector)
          .getPrimitiveWritableObject(value).getTextValue());
    case BINARY:
      return value -> {
        BytesWritable binary = ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(value);
        serializeWrite.writeBinary(binary.getBytesRaw(), binary.getOffset(), binary.getLength());
      };
    case DATE:
      return value -> serializeWrite.writeDate(((DateObjectInspector) inspector)
          .getPrimitiveWritableObject(value).getDays());
    case TIMESTAMP:
      return value -> serializeWrite.writeTimestamp(((TimestampObjectInspector) inspector)
          .getPrimitiveJavaObject(value));
    case INTERVAL_YEAR_MONTH:
      return value -> serializeWrite.writeHiveIntervalYearMonth(((HiveIntervalYearMonthObjectInspector) inspector)
          .getPrimitiveJavaObject(value));
    case INTERVAL_DAY_TIME:
      return value -> serializeWrite.writeHiveIntervalDayTime(((HiveIntervalDayTimeObjectInspector) inspector)
          .getPrimitiveJavaObject(value));
    case DECIMAL:
      return value -> {
        HiveDecimalWritable decimal = ((HiveDecimalObjectInspector) inspector).getPrimitiveWritableObject(value);
        if (decimal == null) {
          serializeWrite.writeNull();
        } else {
          serializeWrite.writeHiveDecimal(decimal, inspector.scale());
        }
      };
    default:
      throw new AssertionError("Unexpected runtime value type " + inspector.getPrimitiveCategory());
    }
  }

  private void writeText(Text text) throws IOException {
    serializeWrite.writeString(text.getBytes(), 0, text.getLength());
  }
}
