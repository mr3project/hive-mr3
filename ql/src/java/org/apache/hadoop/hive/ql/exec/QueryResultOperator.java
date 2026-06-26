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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.QueryResultDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * QueryResultOperator stores query-result rows in memory and commits them to the
 * Tez/MR3 processor context when the task attempt is allowed to commit.
 */
public class QueryResultOperator extends Operator<QueryResultDesc> {
  private static final long serialVersionUID = 1L;

  private transient AbstractSerDe serializer;
  private transient ObjectInspector inputOI;
  private transient Writable recordValue;
  private transient ByteArrayOutputStream writer;
  private transient DataOutputStream dataOut;
  private transient ProcessorContext processorContext;
  private transient int rowSeparator;
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

    if (conf.getTableInfo() == null) {
      throw new HiveException("QueryResultOperator requires a TableDesc");
    }

    try {
      serializer = conf.getTableInfo().getSerDeClass().newInstance();
      serializer.initialize(hconf, conf.getTableInfo().getProperties(), null);
    } catch (InstantiationException | IllegalAccessException | SerDeException e) {
      throw new HiveException("Unable to initialize QueryResultOperator serializer", e);
    }

    inputOI = inputObjInspectors[0];
    writer = new ByteArrayOutputStream();
    dataOut = new DataOutputStream(writer);
    maxBytes = hconf.getLong(QueryResultDesc.QUERY_RESULT_PER_TASK_MAX_BYTES, conf.getMaxBytes());
    rowSeparator = getRowSeparator(conf.getTableInfo().getProperties());

    MapredContext mapredContext = MapredContext.get();
    if (!(mapredContext instanceof TezContext)) {
      throw new HiveException("QueryResultOperator requires TezContext");
    }
    processorContext = ((TezContext) mapredContext).getTezProcessorContext();
    if (processorContext == null) {
      throw new HiveException("QueryResultOperator requires ProcessorContext");
    }
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
        }
      }

      if (dataOut != null) {
        dataOut.flush();
      }

      if (abort) {
        return;
      }

      if (!processorContext.canCommit()) {
        LOG.info("QueryResultOperator is not allowed to commit resultId={}", conf.getResultId());
        return;
      }

      commitDagOutput();
    } catch (IOException | SerDeException e) {
      throw new HiveException("Error closing QueryResultOperator", e);
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
    } else if (writable instanceof BytesWritable) {
      BytesWritable bytes = (BytesWritable) writable;
      dataOut.write(bytes.getBytes(), 0, bytes.getLength());
    } else {
      writable.write(dataOut);
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

  private int getRowSeparator(Properties tableProperties) {
    String rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n");
    try {
      return Byte.parseByte(rowSeparatorString);
    } catch (NumberFormatException e) {
      return rowSeparatorString.charAt(0);
    }
  }

  private void commitDagOutput() throws IOException, HiveException {
    byte[] payload = writer.toByteArray();
    ByteString dagOutput = UnsafeByteOperations.unsafeWrap(payload);
    Method commitMethod = findCommitDagOutput(ByteString.class);
    Object argument = dagOutput;
    if (commitMethod == null) {
      commitMethod = findCommitDagOutput(byte[].class);
      argument = payload;
    }
    if (commitMethod == null) {
      throw new HiveException("ProcessorContext does not support commitDagOutput");
    }

    try {
      commitMethod.invoke(processorContext, argument);
    } catch (IllegalAccessException e) {
      throw new HiveException("Unable to access commitDagOutput on ProcessorContext", e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new HiveException("commitDagOutput failed", cause);
    }
  }

  private Method findCommitDagOutput(Class<?> parameterType) {
    try {
      return processorContext.getClass().getMethod("commitDagOutput", parameterType);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
