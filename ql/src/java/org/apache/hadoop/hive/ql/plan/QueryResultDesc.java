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

package org.apache.hadoop.hive.ql.plan;

import java.util.Objects;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * QueryResultDesc.
 */
@Explain(displayName = "Query Result Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class QueryResultDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  public static final String QUERY_RESULT_PER_TASK_MAX_BYTES =
      "hive.mr3.query.result.per.task.max.bytes";
  public static final long DEFAULT_QUERY_RESULT_PER_TASK_MAX_BYTES = 64L * 1024L * 1024L;

  private String resultId;
  private TableDesc tableInfo;
  private boolean usingBatchingSerDe;
  private long maxBytes = DEFAULT_QUERY_RESULT_PER_TASK_MAX_BYTES;

  public QueryResultDesc() {
  }

  public QueryResultDesc(String resultId, TableDesc tableInfo, boolean usingBatchingSerDe) {
    this(resultId, tableInfo, usingBatchingSerDe, DEFAULT_QUERY_RESULT_PER_TASK_MAX_BYTES);
  }

  public QueryResultDesc(String resultId, TableDesc tableInfo, boolean usingBatchingSerDe,
      long maxBytes) {
    this.resultId = resultId;
    this.tableInfo = tableInfo;
    this.usingBatchingSerDe = usingBatchingSerDe;
    this.maxBytes = maxBytes;
  }

  @Override
  public Object clone() {
    QueryResultDesc ret = new QueryResultDesc(resultId, tableInfo, usingBatchingSerDe, maxBytes);
    ret.setStatistics(getStatistics());
    ret.setTraits(getTraits());
    ret.setOpProps(getOpProps());
    ret.setMemoryNeeded(getMemoryNeeded());
    ret.setMaxMemoryAvailable(getMaxMemoryAvailable());
    ret.setEstimateNumExecutors(getEstimateNumExecutors());
    ret.setRuntimeStatsTmpDir(getRuntimeStatsTmpDir());
    ret.setColumnExprMap(getColumnExprMap());
    ret.setBucketingVersion(getBucketingVersion());
    return ret;
  }

  @Explain(displayName = "result id")
  public String getResultId() {
    return resultId;
  }

  public void setResultId(String resultId) {
    this.resultId = resultId;
  }

  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(TableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }

  @Explain(displayName = "using batching SerDe")
  public boolean isUsingBatchingSerDe() {
    return usingBatchingSerDe;
  }

  public void setUsingBatchingSerDe(boolean usingBatchingSerDe) {
    this.usingBatchingSerDe = usingBatchingSerDe;
  }

  @Explain(displayName = "max bytes")
  public long getMaxBytes() {
    return maxBytes;
  }

  public void setMaxBytes(long maxBytes) {
    this.maxBytes = maxBytes;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (!(other instanceof QueryResultDesc)) {
      return false;
    }
    QueryResultDesc otherDesc = (QueryResultDesc) other;
    return Objects.equals(resultId, otherDesc.resultId)
        && Objects.equals(tableInfo, otherDesc.tableInfo)
        && usingBatchingSerDe == otherDesc.usingBatchingSerDe
        && maxBytes == otherDesc.maxBytes;
  }
}
