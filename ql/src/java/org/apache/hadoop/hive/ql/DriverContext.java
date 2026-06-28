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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for the procedure managed by the Driver.
 */
public class DriverContext {
  private static final Logger LOG = LoggerFactory.getLogger(DriverContext.class);
  // For WebUI.  Kept alive after queryPlan is freed.
  private final QueryDisplay queryDisplay = new QueryDisplay();

  private final QueryState queryState;
  private final QueryInfo queryInfo;
  private final HiveConf conf;
  private final HookRunner hookRunner;

  // Transaction manager the Driver has been initialized with (can be null).
  // If this is set then this Transaction manager will be used during query
  // compilation/execution rather than using the current session's transaction manager.
  // This might be needed in a situation where a Driver is nested within an already
  // running Driver/query - the nested Driver requires a separate transaction manager
  // so as not to conflict with the outer Driver/query which is using the session
  // transaction manager.
  private final HiveTxnManager initTxnManager;

  private QueryPlan plan;
  private Schema schema;

  private FetchTask fetchTask;
  // Transaction manager used for the query. This will be set at compile time based on
  // either initTxnMgr or from the SessionState, in that order.
  private HiveTxnManager txnManager;
  private TxnType txnType = TxnType.DEFAULT;
  private boolean outdatedTxn;
  private StatsSource statsSource;

  // Boolean to store information about whether valid txn list was generated
  // for current query.
  private boolean validTxnListsGenerated;

  private CacheUsage cacheUsage;
  private CacheEntry usedCacheEntry;

  private boolean retrial = false;

  private DataInput resStream;
  private DagOutputResultReader dagOutputResultReader;

  // HS2 operation handle guid string
  private String operationId;
  private String queryErrorMessage;

  public DriverContext(QueryState queryState, QueryInfo queryInfo, HookRunner hookRunner,
      HiveTxnManager initTxnManager) {
    this.queryState = queryState;
    this.queryInfo = queryInfo;
    this.conf = queryState.getConf();
    this.hookRunner = hookRunner;
    this.initTxnManager = initTxnManager;
  }

  public QueryDisplay getQueryDisplay() {
    return queryDisplay;
  }

  public String getQueryId() {
    return queryDisplay.getQueryId();
  }

  public String getQueryString() {
    return queryDisplay.getQueryString();
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public QueryInfo getQueryInfo() {
    return queryInfo;
  }

  public HiveConf getConf() {
    return conf;
  }

  public HookRunner getHookRunner() {
    return hookRunner;
  }

  public HiveTxnManager getInitTxnManager() {
    return initTxnManager;
  }

  public QueryPlan getPlan() {
    return plan;
  }

  public void setPlan(QueryPlan plan) {
    this.plan = plan;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public FetchTask getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  public HiveTxnManager getTxnManager() {
    return txnManager;
  }

  public void setTxnManager(HiveTxnManager txnManager) {
    this.txnManager = txnManager;
  }

  public TxnType getTxnType() {
    return txnType;
  }

  public void setTxnType(TxnType txnType) {
    this.txnType = txnType;
  }

  public boolean isOutdatedTxn() {
    return outdatedTxn;
  }

  public void setOutdatedTxn(boolean outdated) {
    this.outdatedTxn = outdated;
  }

  public StatsSource getStatsSource() {
    return statsSource;
  }

  public void setStatsSource(StatsSource statsSource) {
    this.statsSource = statsSource;
  }

  public boolean isValidTxnListsGenerated() {
    return validTxnListsGenerated;
  }

  public void setValidTxnListsGenerated(boolean validTxnListsGenerated) {
    this.validTxnListsGenerated = validTxnListsGenerated;
  }

  public CacheUsage getCacheUsage() {
    return cacheUsage;
  }

  public void setCacheUsage(CacheUsage cacheUsage) {
    this.cacheUsage = cacheUsage;
  }

  public CacheEntry getUsedCacheEntry() {
    return usedCacheEntry;
  }

  public void setUsedCacheEntry(CacheEntry usedCacheEntry) {
    this.usedCacheEntry = usedCacheEntry;
  }

  public boolean isRetrial() {
    return retrial;
  }

  public void setRetrial(boolean retrial) {
    this.retrial = retrial;
  }

  public DataInput getResStream() {
    LOG.error("DriverContext.getResStream called: queryId={}, contextId={}, hasResStream={}",
        queryState.getQueryId(), System.identityHashCode(this), resStream != null);
    return resStream;
  }

  public void setResStream(DataInput resStream) {
    LOG.error("DriverContext.setResStream called: queryId={}, contextId={}, hadResStream={}, "
            + "hasNewResStream={}, caller={}",
        queryState.getQueryId(), System.identityHashCode(this), this.resStream != null, resStream != null,
        getCallerForDebugLog());
    this.resStream = resStream;
  }

  public void setDagOutputResultReader(DagOutputResultReader dagOutputResultReader) {
    LOG.error("DriverContext.setDagOutputResultReader called: queryId={}, contextId={}, hadReader={}, "
            + "hasNewReader={}, newPayloadCount={}, caller={}",
        queryState.getQueryId(), System.identityHashCode(this), this.dagOutputResultReader != null,
        dagOutputResultReader != null, dagOutputResultReader == null ? -1 : dagOutputResultReader.getPayloadCount(),
        getCallerForDebugLog());
    this.dagOutputResultReader = dagOutputResultReader;
  }

  public DataInput getDagOutputResultStream() {
    LOG.error("DriverContext.getDagOutputResultStream called: queryId={}, contextId={}, hasReader={}",
        queryState.getQueryId(), System.identityHashCode(this), dagOutputResultReader != null);
    if (dagOutputResultReader == null) {
      LOG.error("DriverContext.getDagOutputResultStream returning null: queryId={}, contextId={}, no reader",
          queryState.getQueryId(), System.identityHashCode(this));
      return null;
    }
    DataInput resultStream = dagOutputResultReader.nextStream();
    LOG.error("DriverContext.getDagOutputResultStream returning: queryId={}, contextId={}, hasStream={}",
        queryState.getQueryId(), System.identityHashCode(this), resultStream != null);
    return resultStream;
  }

  public boolean hasDagOutputResultReader() {
    boolean hasReader = dagOutputResultReader != null;
    LOG.error("DriverContext.hasDagOutputResultReader called: queryId={}, contextId={}, hasReader={}",
        queryState.getQueryId(), System.identityHashCode(this), hasReader);
    return hasReader;
  }

  public void resetDagOutputResultReader() {
    LOG.error("DriverContext.resetDagOutputResultReader called: queryId={}, contextId={}, hasReader={}",
        queryState.getQueryId(), System.identityHashCode(this), dagOutputResultReader != null);
    if (dagOutputResultReader != null) {
      dagOutputResultReader.reset();
    }
  }

  private String getCallerForDebugLog() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    for (StackTraceElement element : stackTrace) {
      if (!element.getClassName().equals(Thread.class.getName())
          && !element.getClassName().equals(DriverContext.class.getName())) {
        return element.toString();
      }
    }
    return "unknown";
  }

  public String getOperationId() {
    return operationId;
  }

  public void setOperationId(String operationId) {
    this.operationId = operationId;
  }

  public String getQueryErrorMessage() {
    return queryErrorMessage;
  }

  public void setQueryErrorMessage(String queryErrorMessage) {
    this.queryErrorMessage = queryErrorMessage;
  }
}
