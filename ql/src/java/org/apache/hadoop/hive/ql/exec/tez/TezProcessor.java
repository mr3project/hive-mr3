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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.events.ProcessorHandlerSetupCloseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import com.google.common.base.Throwables;

/**
 * Hive processor for Tez that forms the vertices in Tez and processes the data.
 * Does what ExecMapper and ExecReducer does for hive in MR framework.
 */
public class TezProcessor extends AbstractLogicalIOProcessor {

  /**
   * This provides the ability to pass things into TezProcessor, which is normally impossible
   * because of how Tez APIs are structured. Piggyback on ExecutionContext.
   */
  public static interface Hook {
    void initializeHook(TezProcessor source);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TezProcessor.class);
  protected boolean isMap = false;

  protected RecordProcessor rproc = null;
  private final AtomicBoolean aborted = new AtomicBoolean(false);

  protected JobConf jobConf;

  private static final String CLASS_NAME = TezProcessor.class.getName();
  private final PerfLogger perfLogger = SessionState.getPerfLogger();

  // TODO: Replace with direct call to ProgressHelper, when reliably available.
  private static class ReflectiveProgressHelper {

    Configuration conf;
    Class<?> progressHelperClass = null;
    Object progressHelper = null;

    ReflectiveProgressHelper(Configuration conf,
                             Map<String, LogicalInput> inputs,
                             ProcessorContext processorContext,
                             String processorName) {
      this.conf = conf;
      try {
        progressHelperClass = this.conf.getClassByName("org.apache.tez.common.ProgressHelper");
        progressHelper = progressHelperClass.getDeclaredConstructor(Map.class, ProcessorContext.class, String.class)
                            .newInstance(inputs, processorContext, processorName);
        LOG.debug("ProgressHelper initialized!");
      }
      catch(Exception ex) {
        LOG.warn("Could not find ProgressHelper. " + ex);
      }
    }

    private boolean isValid() {
      return progressHelperClass != null && progressHelper != null;
    }

    void scheduleProgressTaskService(long delay, long period) {
      if (!isValid()) {
        LOG.warn("ProgressHelper uninitialized. Bailing on scheduleProgressTaskService()");
        return;
      }
      try {
        progressHelperClass.getDeclaredMethod("scheduleProgressTaskService", long.class, long.class)
            .invoke(progressHelper, delay, period);
        LOG.debug("scheduleProgressTaskService() called!");
      } catch (Exception exception) {
        LOG.warn("Could not scheduleProgressTaskService.", exception);
      }
    }

    void shutDownProgressTaskService() {
      if (!isValid()) {
        LOG.warn("ProgressHelper uninitialized. Bailing on scheduleProgressTaskService()");
        return;
      }
      try {
        progressHelperClass.getDeclaredMethod("shutDownProgressTaskService").invoke(progressHelper);
        LOG.debug("shutDownProgressTaskService() called!");
      }
      catch (Exception exception) {
        LOG.warn("Could not shutDownProgressTaskService.", exception);
      }
    }
  }

  protected ProcessorContext processorContext;
  private ReflectiveProgressHelper progressHelper;

  protected static final NumberFormat taskIdFormat = NumberFormat.getInstance();
  protected static final NumberFormat jobIdFormat = NumberFormat.getInstance();
  static {
    taskIdFormat.setGroupingUsed(false);
    taskIdFormat.setMinimumIntegerDigits(6);
    jobIdFormat.setGroupingUsed(false);
    jobIdFormat.setMinimumIntegerDigits(4);
  }

  public TezProcessor(ProcessorContext context) {
    super(context);
    ObjectCache.setupObjectRegistry(context);
    OrcFile.setupOrcMemoryManager(context.getTotalMemoryAvailableToTask());
  }

  @Override
  public void close() throws IOException {
    // we have to close in the processor's run method, because tez closes inputs
    // before calling close (TEZ-955) and we might need to read inputs
    // when we flush the pipeline.
      if (progressHelper != null) {
        progressHelper.shutDownProgressTaskService();
      }

    IOContextMap.clearThreadAttempt(getContext().getUniqueIdentifier());
  }

  @Override
  public void handleEvents(List<Event> arg0) {
    // for ProcessorHandlerSetupCloseEvent and Bucket MapJoin, there is exactly one event in the list.
    assert arg0.size() <= 1;
    for (Event event : arg0) {
      if (event instanceof ProcessorHandlerSetupCloseEvent) {
        ProcessorHandlerSetupCloseEvent phEvent = (ProcessorHandlerSetupCloseEvent) event;
        boolean setup = phEvent.getSetup();
        if (setup) {
          IOContextMap.setThreadAttemptId(processorContext.getUniqueIdentifier());
        } else {
          IOContextMap.cleanThreadAttemptId(processorContext.getUniqueIdentifier());
        }
      } else if (event instanceof CustomProcessorEvent) {
        CustomProcessorEvent cpEvent = (CustomProcessorEvent) event;
        ByteBuffer buffer = cpEvent.getPayload();
        // Get int view of the buffer
        IntBuffer intBuffer = buffer.asIntBuffer();
        jobConf.setInt(Constants.LLAP_NUM_BUCKETS, intBuffer.get(0));
        jobConf.setInt(Constants.LLAP_BUCKET_ID, intBuffer.get(1));
      } else {
        LOG.error("Ignoring unknown Event: " + event);
      }
    }
  }

  @Override
  public void initialize() throws IOException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
    Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    this.jobConf = new JobConf(conf);
    this.processorContext = getContext();
    ExecutionContext execCtx = processorContext.getExecutionContext();
    if (execCtx instanceof Hook) {
      ((Hook)execCtx).initializeHook(this);
    }
    setupMRLegacyConfigs(processorContext);

    // use the implementation of IOContextMap for LLAP
    IOContextMap.setThreadAttemptId(processorContext.getUniqueIdentifier());

    String engine = HiveConf.getVar(this.jobConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr3") || engine.equals("tez")) {
      int dagIdId = processorContext.getDagIdentifier();
      String queryId = HiveConf.getVar(this.jobConf, HiveConf.ConfVars.HIVEQUERYID);
      processorContext.setDagShutdownHook(dagIdId,
          new Runnable() {
            public void run() {
              ObjectCacheFactory.removeLlapQueryCache(queryId);
            }
          },
          new org.apache.tez.runtime.api.TaskContext.VertexShutdown() {
            public void run(int vertexIdId) {
              ObjectCacheFactory.removeLlapQueryVertexCache(queryId, vertexIdId);
            }
          });
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
  }

  private void setupMRLegacyConfigs(ProcessorContext processorContext) {
    // Hive "insert overwrite local directory" uses task id as dir name
    // Setting the id in jobconf helps to have the similar dir name as MR
    StringBuilder taskAttemptIdBuilder = new StringBuilder("attempt_");
    taskAttemptIdBuilder.append(processorContext.getApplicationId().getClusterTimestamp())
        .append("_")
        .append(jobIdFormat.format(processorContext.getApplicationId().getId()))
        .append("_");
    if (isMap) {
      taskAttemptIdBuilder.append("m_");
    } else {
      taskAttemptIdBuilder.append("r_");
    }
    taskAttemptIdBuilder.append(taskIdFormat.format(processorContext.getTaskIndex()))
      .append("_")
      .append(processorContext.getTaskAttemptNumber());

    // In MR, mapreduce.task.attempt.id is same as mapred.task.id. Go figure.
    String taskAttemptIdStr = taskAttemptIdBuilder.toString();
    this.jobConf.set("mapred.task.id", taskAttemptIdStr);
    this.jobConf.set("mapreduce.task.attempt.id", taskAttemptIdStr);
    this.jobConf.setInt("mapred.task.partition", processorContext.getTaskIndex());
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {

    if (aborted.get()) {
      return;
    }

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)

    if (LOG.isDebugEnabled()) {
      LOG.debug("Running task: " + getContext().getUniqueIdentifier());
    }

    synchronized (this) {
      // This check isn't absolutely mandatory, given the aborted check outside of the
      // Processor creation.
      if (aborted.get()) {
        return;
      }

      // leverage TEZ-3437: Improve synchronization and the progress report behavior.
      progressHelper = new ReflectiveProgressHelper(jobConf, inputs, getContext(), this.getClass().getSimpleName());


      // There should be no blocking operation in RecordProcessor creation,
      // otherwise the abort operation will not register since they are synchronized on the same
      // lock.
      if (isMap) {
        rproc = new MapRecordProcessor(jobConf, getContext());
      } else {
        rproc = new ReduceRecordProcessor(jobConf, getContext());
      }
    }

    progressHelper.scheduleProgressTaskService(0, 100);
    if (!aborted.get()) {
      initializeAndRunProcessor(inputs, outputs);
    }
    // TODO HIVE-14042. In case of an abort request, throw an InterruptedException
    // implement HIVE-14042 in initializeAndRunProcessor(), not here
  }

  protected void initializeAndRunProcessor(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs)
      throws Exception {
    Throwable originalThrowable = null;

    boolean setLlapCacheCounters = isMap &&
        HiveConf.getVar(this.jobConf, HiveConf.ConfVars.HIVE_EXECUTION_MODE).equals("llap")
        && HiveConf.getBoolVar(this.jobConf, HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS);
    String fragmentId = null;
    if (setLlapCacheCounters) {
      TezDAGID tezDAGID = TezDAGID.getInstance(this.getContext().getApplicationId(), this.getContext().getDagIdentifier());
      TezVertexID tezVertexID = TezVertexID.getInstance(tezDAGID, this.getContext().getTaskVertexIndex());
      TezTaskID tezTaskID = TezTaskID.getInstance(tezVertexID, this.getContext().getTaskIndex());
      TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.getInstance(tezTaskID, this.getContext().getTaskAttemptNumber());
      this.jobConf.set(MRInput.TEZ_MAPREDUCE_TASK_ATTEMPT_ID, tezTaskAttemptID.toString());
      fragmentId = LlapTezUtils.getFragmentId(this.jobConf);
      FragmentCountersMap.registerCountersForFragment(fragmentId, this.processorContext.getCounters());
    }

    try {
      MRTaskReporter mrReporter = new MRTaskReporter(getContext());
      // Init and run are both potentially long, and blocking operations. Synchronization
      // with the 'abort' operation will not work since if they end up blocking on a monitor
      // which does not belong to the lock, the abort will end up getting blocked.
      // Both of these method invocations need to handle the abort call on their own.
      rproc.init(mrReporter, inputs, outputs);
      rproc.run();

      //done - output does not need to be committed as hive does not use outputcommitter
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    } catch (Throwable t) {
      originalThrowable = t;
    } finally {
      if (originalThrowable != null && (originalThrowable instanceof Error ||
        Throwables.getRootCause(originalThrowable) instanceof Error)) {
        LOG.error("Cannot recover from this FATAL error", StringUtils.stringifyException(originalThrowable));
        getContext().reportFailure(TaskFailureType.FATAL, originalThrowable,
                      "Cannot recover from this error");
        ObjectCache.clearObjectRegistry();  // clear thread-local cache which may contain MAP/REDUCE_PLAN
        Utilities.clearWork(jobConf);       // clear thread-local gWorkMap which may contain MAP/REDUCE_PLAN
        if (setLlapCacheCounters) {
          FragmentCountersMap.unregisterCountersForFragment(fragmentId);
        }
        throw new RuntimeException(originalThrowable);
      }

      try {
        if (rproc != null) {
          rproc.close();
        }
      } catch (Throwable t) {
        if (originalThrowable == null) {
          originalThrowable = t;
        }
      }

      if (setLlapCacheCounters) {
        FragmentCountersMap.unregisterCountersForFragment(fragmentId);
      }

      // Invariant: calls to abort() eventually lead to clearing thread-local cache exactly once.
      // 1.
      // rproc.run()/close() may return normally even after abort() is called and rProcLocal.abort() is
      // executed. In such a case, thread-local cache may be in a corrupted state. Hence, we should raise
      // InterruptedException by setting originalThrowable to InterruptedException. In this way, we clear
      // thread-local cache and fail the current TaskAttempt.
      // 2.
      // We set this.aborted to true, so that from now on, abort() is ignored to avoid corrupting thread-local
      // cache (MAP_PLAN/REDUCE_PLAN).

      // 2. set aborted to true
      boolean prevAborted = aborted.getAndSet(true);
      // 1. raise InterruptedException if necessary
      if (prevAborted && originalThrowable == null) {
        originalThrowable = new InterruptedException("abort() was called, but RecordProcessor successfully returned");
      }

      if (originalThrowable != null) {
        LOG.error(StringUtils.stringifyException(originalThrowable));
        ObjectCache.clearObjectRegistry();  // clear thread-local cache which may contain MAP/REDUCE_PLAN
        Utilities.clearWork(jobConf);       // clear thread-local gWorkMap which may contain MAP/REDUCE_PLAN
        if (originalThrowable instanceof InterruptedException) {
          throw (InterruptedException) originalThrowable;
        } else {
          throw new RuntimeException(originalThrowable);
        }
      }
    }
  }

  // abort() can be called after run() has returned without throwing Exception.
  // Calling ObjectCache.clearObjectRegistry() and Utilities.clearWork(jobConf) inside abort() does not make
  // sense because the caller thread is not the same thread that calls run().
  @Override
  public void abort() {
    RecordProcessor rProcLocal = null;
    synchronized (this) {
      LOG.info("Received abort");
      boolean prevAborted = aborted.getAndSet(true);
      if (!prevAborted) {
        rProcLocal = rproc;
      }
    }
    if (rProcLocal != null) {
      LOG.info("Forwarding abort to RecordProcessor");
      rProcLocal.abort();
    } else {
      LOG.info("RecordProcessor not yet setup or already completed. Abort will be ignored");
    }
  }

  /**
   * KVOutputCollector. OutputCollector that writes using KVWriter.
   * Must be initialized before it is used.
   *
   */
  @SuppressWarnings("rawtypes")
  static class TezKVOutputCollector implements OutputCollector {
    private KeyValueWriter writer;
    private final LogicalOutput output;

    TezKVOutputCollector(LogicalOutput logicalOutput) {
      this.output = logicalOutput;
    }

    void initialize() throws Exception {
      this.writer = (KeyValueWriter) output.getWriter();
    }

    @Override
    public void collect(Object key, Object value) throws IOException {
      writer.write(key, value);
    }
  }

  public JobConf getConf() {
    return jobConf;
  }
}
