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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.QueryResultDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.DAGOutputEvent;

/**
 * QueryResultOperator is a distilled FileSinkOperator for user-visible query results.
 *
 * It preserves the FileSinkOperator row serialization path, but replaces the
 * file-backed RecordWriter with an in-memory buffer that is committed to the
 * Tez/MR3 processor context when the task attempt is allowed to commit.
 */
public class QueryResultOperator extends TerminalOperator<QueryResultDesc> {
  private static final long serialVersionUID = 1L;

  private transient AbstractSerDe serializer;
  private transient ObjectInspector inputOI;
  private transient Writable recordValue;
  private transient int rowSeparator;

  private transient ByteArrayOutputStream writer;
  private transient DataOutputStream dataOut;
  private transient ProcessorContext processorContext;
  private transient long maxBytes;

  /** Kryo ctor. */
  protected QueryResultOperator() {
    super();
  }

  public QueryResultOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    try {
      serializer = conf.getTableInfo().getSerDeClass().newInstance();
      serializer.initialize(hconf, conf.getTableInfo().getProperties(), null);
    } catch (InstantiationException | IllegalAccessException | SerDeException e) {
      throw new HiveException("Unable to initialize QueryResultOperator serializer", e);
    }
    inputOI = inputObjInspectors[0];

    writer = new ByteArrayOutputStream();
    dataOut = new DataOutputStream(writer);
    maxBytes = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_MR3_QUERY_RESULT_TASK_MAX_BYTES);
    rowSeparator = HiveIgnoreKeyTextOutputFormat.getRowSeparator(conf.getTableInfo().getProperties());

    MapredContext mapredContext = MapredContext.get();
    processorContext = ((TezContext) mapredContext).getTezProcessorContext();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    runTimeNumRows++;
    try {
      recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (recordValue == null) {
        return;
      }
      writeRecord(recordValue);
      numRows++;
    } catch (SerDeException | IOException e) {
      throw new HiveException("Error writing query result row", e);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    try {
      if (!abort && conf.isUsingBatchingSerDe()) {
        recordValue = serializer.serialize(null, inputOI);
        if (recordValue != null) {
          writeRecord(recordValue);
          numRows++;
        }
      }

      if (dataOut != null) {
        dataOut.flush();
      }

      if (abort) {
        return;
      }

      waitForCanCommit();

      commitDagOutput();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HiveException("Interrupted while waiting to commit QueryResultOperator", e);
    } catch (IOException | SerDeException e) {
      throw new HiveException("Error closing QueryResultOperator", e);
    }
  }

  private void waitForCanCommit() throws IOException, InterruptedException {
    boolean logged = false;
    while (!processorContext.canCommit()) {
      if (!logged) {
        LOG.info("QueryResultOperator is not allowed to commit resultId={}; waiting", conf.getResultId());
        logged = true;
      }
      Thread.sleep(500);
    }
  }

  @Override
  public String getName() {
    return QueryResultOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "QRO";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }

  private void writeRecord(Writable writable) throws IOException, HiveException {
    if (writable instanceof Text) {
      Text text = (Text) writable;
      dataOut.write(text.getBytes(), 0, text.getLength());
    } else {
      // Binary SerDes always write out BytesWritable.
      BytesWritable bytes = (BytesWritable) writable;
      dataOut.write(bytes.getBytesRaw(), 0, bytes.getLength());
    }
    dataOut.write(rowSeparator);
    enforceMaxBytes();
  }

  private void enforceMaxBytes() throws HiveException {
    if (maxBytes >= 0 && writer.size() > maxBytes) {
      throw new HiveException("Query result for resultId=" + conf.getResultId()
          + " exceeded per-task limit " + maxBytes + " bytes");
    }
  }

  private void commitDagOutput() throws IOException {
    byte[] payload = writer.toByteArray();
    if (payload.length > 0) {
      LOG.info("DAG output reported: {}, {} bytes", conf.getResultId(), payload.length);
      ByteString dagOutput = UnsafeByteOperations.unsafeWrap(payload);
      DAGOutputEvent event = DAGOutputEvent.create(conf.getResultId(), dagOutput, (int) numRows);
      processorContext.sendEvents(Collections.singletonList(event));
    }
  }
}
