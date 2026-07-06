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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.DAGOutputEvent;

/** MR3-private FileSinkOperator query-result DAG-output transport helper. */
class QueryResultDagOutputWriter {
  private FileSinkDesc conf;
  private AbstractSerDe serializer;
  private ObjectInspector inputOI;
  private ObjectInspector[] inputObjInspectors;
  private ByteArrayOutputStream writer;
  private DataOutputStream dataOut;
  private ProcessorContext processorContext;
  private long numRows;
  private long maxBytes;
  private int rowSeparator;

  void initialize(Configuration hconf, FileSinkDesc conf, ObjectInspector[] inputObjInspectors)
      throws HiveException {
    this.conf = conf;
    this.inputObjInspectors = inputObjInspectors;
    try {
      serializer = conf.getTableInfo().getSerDeClass().newInstance();
      serializer.initialize(hconf, conf.getTableInfo().getProperties(), null);
    } catch (InstantiationException | IllegalAccessException | SerDeException e) {
      throw new HiveException("Unable to initialize MR3 query-result DAG-output serializer", e);
    }
    inputOI = inputObjInspectors[0];
    writer = new ByteArrayOutputStream();
    dataOut = new DataOutputStream(writer);
    maxBytes = conf.getQueryResultMaxBytes();
    if (maxBytes < 0) {
      maxBytes = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_MR3_QUERY_RESULT_TASK_MAX_BYTES);
    }
    rowSeparator = HiveIgnoreKeyTextOutputFormat.getRowSeparator(conf.getTableInfo().getProperties());

    MapredContext mapredContext = MapredContext.get();
    if (!(mapredContext instanceof TezContext)) {
      throw new HiveException("MR3 query-result DAG-output mode requires Tez/MR3 context");
    }
    processorContext = ((TezContext) mapredContext).getTezProcessorContext();
    if (processorContext == null) {
      throw new HiveException("MR3 query-result DAG-output mode requires processor context");
    }
    if (conf.getQueryResultId() == null || conf.getQueryResultId().isEmpty()) {
      throw new HiveException("MR3 query-result DAG-output mode requires queryResultId");
    }
  }

  void process(Object row, int tag) throws HiveException {
    try {
      Writable recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (recordValue != null) {
        writeRecord(recordValue);
        numRows++;
      }
    } catch (SerDeException | IOException e) {
      throw new HiveException("Error writing MR3 query-result DAG-output row", e);
    }
  }

  void close(boolean abort) throws HiveException {
    try {
      if (!abort && conf.isUsingBatchingSerDe()) {
        Writable recordValue = serializer.serialize(null, inputOI);
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
      throw new HiveException("Interrupted while committing MR3 query-result DAG output", e);
    } catch (IOException | SerDeException e) {
      throw new HiveException("Error closing MR3 query-result DAG-output writer", e);
    }
  }

  private void waitForCanCommit() throws IOException, InterruptedException {
    while (!processorContext.canCommit()) {
      Thread.sleep(500);
    }
  }

  private void commitDagOutput() throws IOException {
    byte[] payload = writer.toByteArray();
    if (payload.length == 0) {
      return;
    }
    ByteString dagOutput = UnsafeByteOperations.unsafeWrap(payload);
    DAGOutputEvent event = DAGOutputEvent.create(conf.getQueryResultId(), dagOutput, (int) numRows);
    processorContext.sendEvents(Collections.singletonList(event));
  }

  private void writeRecord(Writable writable) throws IOException, HiveException {
    if (conf.isUsingBatchingSerDe()) {
      writeBinaryRecord(writable);
    } else {
      writeTextRecord(writable);
    }
    enforceMaxBytes();
  }

  private void writeTextRecord(Writable writable) throws IOException {
    if (writable instanceof Text) {
      Text text = (Text) writable;
      dataOut.write(text.getBytes(), 0, text.getLength());
    } else if (writable instanceof BytesWritable) {
      BytesWritable bytes = (BytesWritable) writable;
      dataOut.write(bytes.getBytesRaw(), 0, bytes.getLength());
    } else {
      throw new IOException("Unsupported query-result writable: " + writable.getClass().getName());
    }
    dataOut.write(rowSeparator);
  }

  private void writeBinaryRecord(Writable writable) throws IOException {
    if (writable instanceof BytesWritable) {
      BytesWritable bytes = (BytesWritable) writable;
      dataOut.writeInt(bytes.getLength());
      dataOut.write(bytes.getBytesRaw(), 0, bytes.getLength());
    } else if (writable instanceof Text) {
      Text text = (Text) writable;
      dataOut.writeInt(text.getLength());
      dataOut.write(text.getBytes(), 0, text.getLength());
    } else {
      throw new IOException("Unsupported binary query-result writable: " + writable.getClass().getName());
    }
  }

  private void enforceMaxBytes() throws HiveException {
    if (maxBytes >= 0 && writer.size() > maxBytes) {
      throw new HiveException("Query result for resultId=" + conf.getQueryResultId()
          + " exceeded per-task limit " + maxBytes + " bytes");
    }
  }
}
