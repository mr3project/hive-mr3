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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.DynamicValueRegistry;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.api.LogicalInputEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicValueRegistryTez implements DynamicValueRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicValueRegistryTez.class);

  public static class RegistryConfTez extends RegistryConf {
    public Configuration conf;
    public BaseWork baseWork;
    public ProcessorContext processorContext;
    public Map<String, LogicalInput> inputs;

    public RegistryConfTez(Configuration conf, BaseWork baseWork,
        ProcessorContext processorContext, Map<String, LogicalInput> inputs) {
      super();
      this.conf = conf;
      this.baseWork = baseWork;
      this.processorContext = processorContext;
      this.inputs = inputs;
    }
  }

  static class NullValue {
  }

  static final NullValue NULL_VALUE = new NullValue();

  protected Map<String, Object> values = new ConcurrentHashMap<>();

  public DynamicValueRegistryTez() {
  }

  @Override
  public Object getValue(String key) {
    if (!values.containsKey(key)) {
      throw new NoDynamicValuesException("Value does not exist in registry: " + key);
    }
    Object val = values.get(key);

    if (val == NULL_VALUE) {
      return null;
    }
    return val;
  }

  protected void setValue(String key, Object value) {
    if (value == null) {
      // ConcurrentHashMap does not allow null - use a substitute value.
      values.put(key, NULL_VALUE);
    } else {
      values.put(key, value);
    }
  }

  @Override
  public void init(RegistryConf conf) throws Exception {
    RegistryConfTez rct = (RegistryConfTez) conf;

    for (String inputSourceName : rct.baseWork.getInputSourceToRuntimeValuesInfo().keySet()) {
      LOG.info("Runtime value source: " + inputSourceName);

      LogicalInputEdge runtimeValueInput = (LogicalInputEdge) rct.inputs.get(inputSourceName);
      RuntimeValuesInfo runtimeValuesInfo = rct.baseWork.getInputSourceToRuntimeValuesInfo().get(inputSourceName);

      // The table descriptor supplies the schema; runtime values use the fast wire format.
      AbstractSerDe serDe = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getSerDeClass(), null);
      serDe.initialize(rct.conf, runtimeValuesInfo.getTableDesc().getProperties(), null);
      StructTypeInfo rowTypeInfo =
          (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(serDe.getObjectInspector());
      TypeInfo[] fieldTypes = rowTypeInfo.getAllStructFieldTypeInfos().toArray(new TypeInfo[0]);
      for (TypeInfo fieldType : fieldTypes) {
        assert fieldType.getCategory() == Category.PRIMITIVE;
      }
      LazyBinaryDeserializeRead deserializeRead = new LazyBinaryDeserializeRead(fieldTypes, false);
      ObjectInspector inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
      Object[] row = new Object[fieldTypes.length];

      // Set up col expressions for the dynamic values using this input
      List<ExprNodeEvaluator> colExprEvaluators = new ArrayList<ExprNodeEvaluator>();
      for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
        ExprNodeEvaluator exprEval = ExprNodeEvaluatorFactory.get(expr, null);
        exprEval.initialize(inspector);
        colExprEvaluators.add(exprEval);
      }

      runtimeValueInput.start();
      List<Input> inputList = new ArrayList<Input>();
      inputList.add(runtimeValueInput);
      rct.processorContext.waitForAllInputsReady(inputList);

      KeyValueReaderEdge kvReader = (KeyValueReaderEdge) runtimeValueInput.getReader();
      long rowCount = 0;
      while (kvReader.next()) {
        BytesWritable value = kvReader.getCurrentValue();
        deserializeRead.set(value.getBytesRaw(), value.getOffset(), value.getLength());
        for (int fieldIndex = 0; fieldIndex < fieldTypes.length; ++fieldIndex) {
          row[fieldIndex] = deserializeRead.readNextField()
              ? readField(deserializeRead, (PrimitiveTypeInfo) fieldTypes[fieldIndex]) : null;
        }
        rowCount++;
        for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
          // Read each expression and save it to the value registry
          ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
          Object val = eval.evaluate(row);
          setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
        }
      }
      // For now, expecting a single row (min/max, aggregated bloom filter), or no rows
      if (rowCount == 0) {
        LOG.debug("No input rows from " + inputSourceName + ", filling dynamic values with nulls");
        for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
          ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
          setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), null);
        }
      } else if (rowCount > 1) {
        throw new IllegalStateException("Expected 0 or 1 rows from " + inputSourceName + ", got " + rowCount);
      }
    }
  }

  private static Object readField(LazyBinaryDeserializeRead reader, PrimitiveTypeInfo typeInfo) {
    // Copy each field out of the reader's shared scratch storage.
    switch (typeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return new BooleanWritable(reader.currentBoolean);
    case BYTE:
      return new ByteWritable(reader.currentByte);
    case SHORT:
      return new ShortWritable(reader.currentShort);
    case INT:
      return new IntWritable(reader.currentInt);
    case LONG:
      return new LongWritable(reader.currentLong);
    case FLOAT:
      return new FloatWritable(reader.currentFloat);
    case DOUBLE:
      return new DoubleWritable(reader.currentDouble);
    case STRING:
      Text text = new Text();
      text.set(reader.currentBytes, reader.currentBytesStart, reader.currentBytesLength);
      return text;
    case CHAR:
      HiveCharWritable hiveChar = new HiveCharWritable();
      hiveChar.set(readString(reader), ((CharTypeInfo) typeInfo).getLength());
      return hiveChar;
    case VARCHAR:
      HiveVarcharWritable hiveVarchar = new HiveVarcharWritable();
      hiveVarchar.set(readString(reader), ((VarcharTypeInfo) typeInfo).getLength());
      return hiveVarchar;
    case BINARY:
      BytesWritable binary = new BytesWritable();
      binary.set(reader.currentBytes, reader.currentBytesStart, reader.currentBytesLength);
      return binary;
    case DATE:
      return new DateWritableV2(reader.currentDateWritable);
    case TIMESTAMP:
      return new TimestampWritableV2(reader.currentTimestampWritable);
    case INTERVAL_YEAR_MONTH:
      return new HiveIntervalYearMonthWritable(reader.currentHiveIntervalYearMonthWritable);
    case INTERVAL_DAY_TIME:
      return new HiveIntervalDayTimeWritable(reader.currentHiveIntervalDayTimeWritable);
    case DECIMAL:
      return new HiveDecimalWritable(reader.currentHiveDecimalWritable);
    default:
      throw new AssertionError("Unexpected runtime value type " + typeInfo);
    }
  }

  private static String readString(LazyBinaryDeserializeRead reader) {
    return new String(reader.currentBytes, reader.currentBytesStart, reader.currentBytesLength,
        StandardCharsets.UTF_8);
  }
}
