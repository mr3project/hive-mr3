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
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchDeserializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.ReaderEdge;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.api.KeyValueReaderEdgeVector;
import org.apache.tez.runtime.library.api.KeyValueReaderEdgeVector.NextResult;
import org.apache.tez.runtime.library.api.KeyValuesReaderEdge;
import org.apache.tez.runtime.library.api.LogicalInputEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

      // Setup deserializer/obj inspectors for the incoming data source
      AbstractSerDe serDe = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getSerDeClass(), null);
      serDe.initialize(rct.conf, runtimeValuesInfo.getTableDesc().getProperties(), null);
      ObjectInspector inspector = serDe.getObjectInspector();

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

      ReaderEdge reader = runtimeValueInput.getReader();
      long rowCount;
      if (reader instanceof KeyValuesReaderEdge) {
        rowCount = consumeGroupedKeyValues((KeyValuesReaderEdge) reader, serDe, runtimeValuesInfo,
            colExprEvaluators);
      } else {
        KeyValueReaderEdge kvReader = (KeyValueReaderEdge) reader;
        if (kvReader instanceof KeyValueReaderEdgeVector) {
          KeyValueReaderEdgeVector vectorReader = (KeyValueReaderEdgeVector) kvReader;
          NextResult nextResult = vectorReader.nextVectorBatchAware();
          if (nextResult == NextResult.VECTOR_BATCH) {
            rowCount = consumeVectorBatches(kvReader, vectorReader, runtimeValuesInfo, inspector);
          } else if (nextResult == NextResult.KEY_VALUE) {
            // KEY_VALUE is only a mode probe and does not select the first ordinary record.
            rowCount = consumeKeyValues(kvReader, serDe, runtimeValuesInfo, colExprEvaluators);
          } else if (nextResult == NextResult.END_OF_INPUT) {
            rowCount = 0;
          } else {
            throw new IOException("Unexpected vector-aware next result " + nextResult);
          }
        } else {
          rowCount = consumeKeyValues(kvReader, serDe, runtimeValuesInfo, colExprEvaluators);
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

  private long consumeGroupedKeyValues(KeyValuesReaderEdge kvReader, AbstractSerDe serDe,
      RuntimeValuesInfo runtimeValuesInfo, List<ExprNodeEvaluator> colExprEvaluators)
      throws Exception {
    long rowCount = 0;
    while (kvReader.next()) {
      for (BytesWritable value : kvReader.getCurrentValues()) {
        Object row = serDe.deserializeBytesWritable(value);
        evaluateRow(row, runtimeValuesInfo, colExprEvaluators);
        rowCount++;
      }
    }
    return rowCount;
  }

  private long consumeKeyValues(KeyValueReaderEdge kvReader, AbstractSerDe serDe,
      RuntimeValuesInfo runtimeValuesInfo, List<ExprNodeEvaluator> colExprEvaluators)
      throws Exception {
    long rowCount = 0;
    while (kvReader.next()) {
      Object row = serDe.deserializeBytesWritable(kvReader.getCurrentValue());
      evaluateRow(row, runtimeValuesInfo, colExprEvaluators);
      rowCount++;
    }
    return rowCount;
  }

  private long consumeVectorBatches(KeyValueReaderEdge kvReader, KeyValueReaderEdgeVector vectorReader,
      RuntimeValuesInfo runtimeValuesInfo, ObjectInspector inspector) throws Exception {
    StructObjectInspector structInspector = (StructObjectInspector) inspector;
    TypeInfo[] typeInfos = VectorizedBatchUtil.typeInfosFromStructObjectInspector(structInspector);
    VectorizedRowBatch batch = new VectorizedRowBatch(typeInfos.length);
    for (int colIdx = 0; colIdx < typeInfos.length; ++colIdx) {
      batch.cols[colIdx] = VectorizedBatchUtil.createColumnVector(typeInfos[colIdx]);
    }

    VectorShuffleBatchDeserializer deserializer = new VectorShuffleBatchDeserializer();
    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(typeInfos);
    Object[] row = new Object[typeInfos.length];
    ObjectInspector vectorRowInspector = ObjectInspectorUtils.getStandardObjectInspector(
        structInspector, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
    List<ExprNodeEvaluator> vectorEvaluators = createEvaluators(runtimeValuesInfo, vectorRowInspector);

    long rowCount = 0;
    NextResult nextResult = NextResult.VECTOR_BATCH;
    while (nextResult == NextResult.VECTOR_BATCH) {
      deserializer.deserialize(kvReader.getCurrentValue(), batch, typeInfos.length);
      for (int logicalIndex = 0; logicalIndex < batch.size; ++logicalIndex) {
        int batchIndex = batch.selectedInUse ? batch.selected[logicalIndex] : logicalIndex;
        vectorExtractRow.extractRow(batch, batchIndex, row);
        evaluateRow(row, runtimeValuesInfo, vectorEvaluators);
        rowCount++;
      }
      nextResult = vectorReader.nextVectorBatchAware();
    }
    if (nextResult != NextResult.END_OF_INPUT) {
      throw new IOException("Vector-aware reader changed mode to " + nextResult);
    }
    return rowCount;
  }

  private List<ExprNodeEvaluator> createEvaluators(RuntimeValuesInfo runtimeValuesInfo,
      ObjectInspector inspector) throws Exception {
    List<ExprNodeEvaluator> evaluators = new ArrayList<ExprNodeEvaluator>();
    for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr, null);
      evaluator.initialize(inspector);
      evaluators.add(evaluator);
    }
    return evaluators;
  }

  private void evaluateRow(Object row, RuntimeValuesInfo runtimeValuesInfo,
      List<ExprNodeEvaluator> colExprEvaluators) throws Exception {
    for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
      // Read each expression and save it to the value registry
      ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
      Object val = eval.evaluate(row);
      setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
    }
  }

}
