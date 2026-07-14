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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.tez.tools.KeyValueInputMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan Just pump the records
 * through the query plan.
 */

public class MapRecordSource implements RecordSource {

  public static final Logger LOG = LoggerFactory.getLogger(MapRecordSource.class);
  private ExecMapperContext execContext = null;
  private AbstractMapOperator mapOp = null;
  private KeyValueReader reader = null;
  private final boolean grouped = false;
  private int debugRows = 0;

  // Flush the last record when reader is out of records
  private boolean flushLastRecord = false;

  void init(JobConf jconf, AbstractMapOperator mapOp, KeyValueReader reader) throws IOException {
    execContext = mapOp.getExecContext();
    this.mapOp = mapOp;
    if (reader instanceof KeyValueInputMerger) {
      KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
      kvMerger.setIOCxt(execContext.getIoCxt());
    }
    this.reader = reader;
  }

  @Override
  public final boolean isGrouped() {
    return grouped;
  }

  @Override
  public void setFlushLastRecord(boolean flushLastRecord) {
    this.flushLastRecord = flushLastRecord;
  }

  @Override
  public boolean pushRecord() throws HiveException {
    execContext.resetRow();

    try {
      if (reader.next()) {
        Object value;
        try {
          value = reader.getCurrentValue();
        } catch (IOException e) {
          closeReader();
          throw new HiveException(e);
        }
        return processRow(value);
      } else if (flushLastRecord) {
        mapOp.flushRecursive();
      }
    } catch (IOException e) {
      closeReader();
      throw new HiveException(e);
    }
    return false;
  }

  private boolean processRow(Object value) {
    try {
      if (debugRows < 10) {
        LOG.info("ORC_DEBUG MapRecordSource row {} class {} value {}", debugRows,
            value == null ? null : value.getClass().getName(), summarizeValue(value));
        debugRows++;
      }
      if (mapOp.getDone()) {
        return false; // done
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mapOp.process((Writable) value);
      }
    } catch (Throwable e) {
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        LOG.error("Failed to process row", e);
        closeReader();
        throw new RuntimeException(e);
      }
    }
    return true; // give me more
  }

  private static String summarizeValue(Object value) {
    if (value instanceof VectorizedRowBatch) {
      return summarizeBatch((VectorizedRowBatch) value);
    }
    return String.valueOf(value);
  }

  private static String summarizeBatch(VectorizedRowBatch batch) {
    StringBuilder sb = new StringBuilder();
    sb.append("size=").append(batch.size)
        .append(", projectionSize=").append(batch.projectionSize)
        .append(", projectedColumns=").append(Arrays.toString(
            Arrays.copyOf(batch.projectedColumns, batch.projectionSize)))
        .append(", cols=[");
    int maxColumns = Math.min(batch.cols.length, 8);
    for (int c = 0; c < maxColumns; c++) {
      if (c > 0) {
        sb.append("; ");
      }
      sb.append(c).append(":").append(summarizeColumn(batch.cols[c], batch, 5));
    }
    if (batch.cols.length > maxColumns) {
      sb.append("; ...");
    }
    sb.append("]");
    return sb.toString();
  }

  private static String summarizeColumn(ColumnVector column, VectorizedRowBatch batch, int maxRows) {
    if (column == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder(column.getClass().getSimpleName());
    sb.append("(noNulls=").append(column.noNulls)
        .append(", isRepeating=").append(column.isRepeating)
        .append(", values=");
    int rowCount = Math.min(batch.size, maxRows);
    sb.append("[");
    for (int r = 0; r < rowCount; r++) {
      if (r > 0) {
        sb.append(",");
      }
      int row = batch.selectedInUse ? batch.selected[r] : r;
      int vectorIndex = column.isRepeating ? 0 : row;
      sb.append(formatColumnValue(column, vectorIndex));
    }
    if (batch.size > rowCount) {
      sb.append(",...");
    }
    sb.append("])");
    return sb.toString();
  }

  private static String formatColumnValue(ColumnVector column, int row) {
    if (!column.noNulls && column.isNull[row]) {
      return "null";
    } else if (column instanceof LongColumnVector) {
      return Long.toString(((LongColumnVector) column).vector[row]);
    } else if (column instanceof DoubleColumnVector) {
      return Double.toString(((DoubleColumnVector) column).vector[row]);
    } else if (column instanceof BytesColumnVector) {
      BytesColumnVector bytesColumn = (BytesColumnVector) column;
      return new String(bytesColumn.vector[row], bytesColumn.start[row], bytesColumn.length[row],
          StandardCharsets.UTF_8);
    }
    return column.getClass().getSimpleName();
  }

  private void closeReader() {
    if (!(reader instanceof MRReader)) {
      LOG.warn("Cannot close " + (reader == null ? null : reader.getClass()));
      return;
    }
    if (reader instanceof KeyValueInputMerger) {
      // cleanup
      KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
      kvMerger.clean();
    }

    LOG.info("Closing MRReader on error");
    MRReader mrReader = (MRReader)reader;
    try {
      mrReader.close();
    } catch (IOException ex) {
      LOG.error("Failed to close the reader; ignoring", ex);
    }
  }

}
