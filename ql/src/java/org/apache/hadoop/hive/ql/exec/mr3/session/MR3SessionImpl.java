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

package org.apache.hadoop.hive.ql.exec.mr3.session;

import com.datamonad.mr3.api.LocalResourcePayload;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr3.DAGUtils;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3Client;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3Client.MR3ClientState;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3ClientFactory;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;
import com.datamonad.mr3.api.common.MR3Conf$;
import com.datamonad.mr3.api.common.MR3ConfBuilder;
import com.datamonad.mr3.common.fs.StagingDirUtils;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MR3SessionImpl implements MR3Session {

  private static final String CLASS_NAME = MR3SessionImpl.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(MR3Session.class);
  private static final String MR3_DIR = "_mr3_session_dir";
  private static final String MR3_AM_STAGING_DIR = "staging";

  private static final String MR3_SHARED_SESSION_ID = "MR3_SHARED_SESSION_ID";

  private final boolean shareMr3Session;
  private final String sessionId;
  private final String sessionUser;

  // set in start() and close()
  private HiveConf sessionConf;
  // read in submit(), isRunningFromApplicationReport(), getEstimateNumTasksOrNodes()
  private HiveMR3Client hiveMr3Client;

  private ApplicationId appId;

  // invariant: used only if shareMr3Session == true
  private boolean useGlobalMr3SessionIdFromEnv;

  // set in start() and close()
  // read from MR3Task thread via getSessionScratchDir()
  private Path sessionScratchDir;

  // updated in start(), close(), and submit()
  // via updateAmLocalResources()
  private final Map<String, LocalResource> amLocalResources = new HashMap<String, LocalResource>();
  // keep digests only needed after MR3 accepts LocalResources
  private final Map<String, ByteString> amLocalResourceDigests = new HashMap<>();
  // Session initialization resources are localized by YARN.
  // Empty digests reserve their localized names so DAG resources cannot override hive-exec.jar or HIVE_MR3_AUX_JARS.
  private Map<String, LocalResource> sessionLocalResources = new HashMap<>();
  private Map<String, ByteString> sessionLocalResourceDigests = new HashMap<>();

  DAGUtils dagUtils = DAGUtils.getInstance();

  private volatile boolean alreadyExecutedAnyDag = false;

  public void setAlreadyExecutedAnyDag() {
    alreadyExecutedAnyDag = true;
  }

  // Cf. MR3SessionImpl.sessionId != HiveConf.HIVESESSIONID
  private String makeSessionId() {
    if (shareMr3Session) {
      String globalMr3SessionIdFromEnv = System.getenv(MR3_SHARED_SESSION_ID);
      useGlobalMr3SessionIdFromEnv = globalMr3SessionIdFromEnv != null && !globalMr3SessionIdFromEnv.isEmpty();
      if (useGlobalMr3SessionIdFromEnv) {
        return globalMr3SessionIdFromEnv;
      } else {
        return UUID.randomUUID().toString();
      }
    } else {
      return UUID.randomUUID().toString();
    }
  }

  public MR3SessionImpl(boolean shareMr3Session, String sessionUser) {
    this.shareMr3Session = shareMr3Session;
    this.sessionId = makeSessionId();
    this.sessionUser = sessionUser;
  }

  public String getSessionUser() {
    return this.sessionUser;
  }

  @Override
  public synchronized void start(HiveConf conf) throws HiveException {
    this.sessionConf = conf;
    try {
      setupHiveMr3Client(conf);

      LOG.info("Starting HiveMR3Client");
      ApplicationId appId = hiveMr3Client.start();

      LOG.info("Waiting until MR3Client starts and transitions to Ready: " + appId);
      waitUntilMr3ClientReady();

      this.appId = appId;
    } catch (Exception e) {
      LOG.error("Failed to start MR3 Session", e);
      close(true);
      throw new HiveException("Failed to create or start MR3Client", e);
    }
  }

  public synchronized void connect(HiveConf hiveConf, ApplicationId appId) throws HiveException {
    this.sessionConf = hiveConf;
    try {
      setupHiveMr3Client(hiveConf);

      LOG.info("Connecting HiveMR3Client: " + appId);
      hiveMr3Client.connect(appId);

      LOG.info("Waiting until MR3Client transitions to Ready: " + appId);
      waitUntilMr3ClientReady();

      this.appId = appId;
    } catch (Exception e) {
      LOG.error("Failed to connect MR3 Session", e);
      close(false);
      throw new HiveException("Failed to connect MR3Client", e);
    }
  }

  @Override
  public synchronized ApplicationId getApplicationId() {
    return this.appId;
  }

  private void setupHiveMr3Client(HiveConf hiveConf) throws Exception {
    sessionScratchDir = createSessionScratchDir(sessionId);
    setAmStagingDir(sessionScratchDir);

    // 1. read hiveJarLocalResources

    // getSessionInitJars() returns hive-exec.jar + HIVE_MR3_AUX_JARS
    List<LocalResource> hiveJarLocalResources =
        dagUtils.localizeTempFiles(sessionScratchDir, hiveConf, dagUtils.getMr3SessionInitJars(hiveConf));
    sessionLocalResources = dagUtils.convertLocalResourceListToMap(hiveJarLocalResources);
    sessionLocalResourceDigests = new HashMap<>();
    for (String name : sessionLocalResources.keySet()) {
      sessionLocalResourceDigests.put(name, ByteString.EMPTY);
    }

    Credentials sessionCredentials = null;  // null okay because it is passed to scala Option()
    if (dagUtils.shouldAddPathsToCredentials(hiveConf)) {
      sessionCredentials = new Credentials();
      Set<Path> allPaths = new HashSet<Path>();
      for (LocalResource lr: sessionLocalResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      dagUtils.addPathsToCredentials(sessionCredentials, allPaths, hiveConf);
    }

    LOG.info("Creating HiveMR3Client (id: " + sessionId + ", scratch dir: " + sessionScratchDir + ")");
    hiveMr3Client = HiveMR3ClientFactory.createHiveMr3Client(
        sessionId, sessionCredentials, sessionLocalResources, hiveConf);

    // These resources are already installed through the session initialization path.
    // Keep sentinel entries in the AM namespace solely to reject conflicting user resources.
    amLocalResources.putAll(sessionLocalResources);
    amLocalResourceDigests.putAll(sessionLocalResourceDigests);
  }

  private void setAmStagingDir(Path sessionScratchDir) {
    Path amStagingDir = new Path(sessionScratchDir, MR3_AM_STAGING_DIR);
    sessionConf.set(MR3Conf$.MODULE$.MR3_AM_STAGING_DIR(), amStagingDir.toUri().toString());
    // amStagingDir is created by MR3 in ApplicationSubmissionContextBuilder.build()
  }

  /**
   * createSessionScratchDir creates a temporary directory in the scratchDir folder to
   * be used with mr3. Assumes scratchDir exists.
   */
  private Path createSessionScratchDir(String sessionId) throws IOException {
    //TODO: ensure this works in local mode, and creates dir on local FS
    // MR3 needs its own scratch dir (per session)
    Path mr3SessionScratchDir = new Path(SessionState.get().getHdfsScratchDirURIString(), MR3_DIR);
    mr3SessionScratchDir = new Path(mr3SessionScratchDir, sessionId);
    FileSystem fs = mr3SessionScratchDir.getFileSystem(sessionConf);
    FsPermission sessionScratchDirPermission = shareMr3Session
        ? new FsPermission(SessionState.SESSION_SCRATCH_DIR_PERMISSION)
        : new FsPermission(HiveConf.getVar(sessionConf, HiveConf.ConfVars.SCRATCH_DIR_PERMISSION));
    Utilities.createDirsWithPermission(
        sessionConf, mr3SessionScratchDir, sessionScratchDirPermission, true);
    // Make sure the path is normalized.
    FileStatus dirStatus = DAGUtils.validateTargetDir(mr3SessionScratchDir, sessionConf);
    assert dirStatus != null;

    mr3SessionScratchDir = dirStatus.getPath();
    LOG.info("Created MR3 Session Scratch Dir: " + mr3SessionScratchDir);

    // don't keep the directory around on non-clean exit if necessary
    if (shareMr3Session) {
      if (useGlobalMr3SessionIdFromEnv) {
        // because session scratch directory is potentially shared by other HS2 instances
        LOG.info("Do not delete session scratch directory on non-clean exit");
      } else {
        // TODO: currently redundant because close() calls cleanupSessionScratchDir()
        fs.deleteOnExit(mr3SessionScratchDir);  // because Beeline cannot connect to this HS2 instance
      }
    } else {
      // TODO: currently redundant because close() calls cleanupSessionScratchDir()
      fs.deleteOnExit(mr3SessionScratchDir);  // because Beeline cannot connect to this HS2 instance
    }

    return mr3SessionScratchDir;
  }

  // handle hiveMr3Client and sessionScratchDir independently because close() can be called from start()
  // can be called several times
  @Override
  public synchronized void close(boolean terminateApplication) {
    if (hiveMr3Client != null) {
      hiveMr3Client.close(terminateApplication);
    }
    hiveMr3Client = null;

    amLocalResources.clear();
    amLocalResourceDigests.clear();
    sessionLocalResources.clear();
    sessionLocalResourceDigests.clear();

    // Requirement: useGlobalMr3SessionIdFromEnv == true if and only if on 'Yarn with HA' or on K8s
    //
    // On Yarn without HA:
    //   invariant: terminateApplication == true
    //   delete <sessionScratchDir> because Application is unknown to any other HiveServer2 instance
    // On Yarn with HA and with terminateApplication == true;
    //   delete <sessionScratchDir>/staging/.mr3/<application ID>
    //   Cf. <sessionScratchDir> itself should be deleted by the admin user.
    // On K8s:
    //   <sessionScratchDir> is shared by all HS2 instances.
    //   We should not delete <sessionScratchDir> because it is shared by the next Application (== Pod).
    //   hence, same as the case of 'On Yarn with HA'
    //
    // The following code implements the above logic by inspecting useGlobalMr3SessionIdFromEnv.
    if (sessionScratchDir != null && terminateApplication) {
      if (shareMr3Session) {
        if (useGlobalMr3SessionIdFromEnv) {
          cleanupStagingDir();
        } else {
          cleanupSessionScratchDir();
        }
      } else {
        cleanupSessionScratchDir();
      }
    }

    sessionConf = null;
  }

  private void cleanupSessionScratchDir() {
    dagUtils.cleanMr3Dir(sessionScratchDir, sessionConf);
    sessionScratchDir = null;
  }

  private void cleanupStagingDir() {
    // getApplicationId() in getStagingDir() may return null because appId is set at the end of start()/connect()
    if (getApplicationId() != null) {
      dagUtils.cleanMr3Dir(getStagingDir(), sessionConf);
    }
    sessionScratchDir = null;
  }

  private Path getStagingDir() {
    Path baseStagingDir = new Path(sessionScratchDir, MR3_AM_STAGING_DIR);
    return StagingDirUtils.getSystemStagingDirFromBaseStagingDir(baseStagingDir, getApplicationId().toString());
  }

  public synchronized Path getSessionScratchDir() {
    return sessionScratchDir;
  }

  @Override
  public MR3JobRef submit(
      DAG dag,
      Map<String, LocalResource> newAmLocalResources,
      Map<String, LocalResourcePayload> newAmLocalResourcePayloads,
      Configuration mr3TaskConf,
      Map<String, BaseWork> workMap,
      Context ctx,
      AtomicBoolean isShutdown,
      PerfLogger perfLogger) throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);

    HiveMR3Client currentHiveMr3Client;
    Map<String, LocalResource> addtlAmLocalResources = new HashMap<>();
    Map<String, LocalResourcePayload> addtlLocalResourcePayloads = new HashMap<>();
    Map<String, LocalResource> currentSessionLocalResources = new HashMap<>();
    Map<String, ByteString> currentSessionLocalResourceDigests = new HashMap<>();
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
      if (currentHiveMr3Client != null) {
        // close() has not been called
        addtlAmLocalResources = getAdditionalAmLocalResources(
            newAmLocalResources, newAmLocalResourcePayloads);
        for (String name : addtlAmLocalResources.keySet()) {
          addtlLocalResourcePayloads.put(name, newAmLocalResourcePayloads.get(name));
        }
        currentSessionLocalResources.putAll(sessionLocalResources);
        currentSessionLocalResourceDigests.putAll(sessionLocalResourceDigests);
      }
    }

    LOG.info("Checking if MR3 Session is open");
    // isOpen() is potentially effect-ful. Note that it eventually calls MR3SessionClient.getSessionStatus()
    // which in turn calls DAGClientRPC.getSessionStatus(). If DAGClientRPC.proxy is set to null,
    // DAGClientRPC.getSessionStatus() creates a new Proxy. This can happen if DAGAppMaster was killed by
    // the user and thus the previous RPC call failed, thus calling DAGClientRPC.stopProxy().
    Preconditions.checkState(isOpen(currentHiveMr3Client), "MR3 Session is not open");

    // still close() can be called at any time (from MR3SessionManager.getNewMr3SessionIfNotAlive())

    String dagUser = UserGroupInformation.getCurrentUser().getShortUserName();
    MR3Conf dagConf = createDagConf(mr3TaskConf, dagUser, dag.getQueryId(), dag.getCommonJobConf());

    // preserve the YARN-localized session resources in the DAG resource namespace
    // without adding payloads to SubmitDagRequestProto
    dag.addLocalResourcesWithDigests(currentSessionLocalResources, currentSessionLocalResourceDigests);

    // sessionConf is not passed to MR3; only dagConf is passed to MR3 as a component of DAGProto.dagConf.
    String submitter = SessionState.get().getUserName();
    if (submitter == null) {
      submitter = "(unknown)";
    }
    DAGAPI.DAGProto dagProto = dag.createDagProto(mr3TaskConf, dagConf, submitter, alreadyExecutedAnyDag);

    Map<String, LocalResourcePayload> submitPayloads = dag.getSubmitLocalResourcePayloads();
    submitPayloads.putAll(addtlLocalResourcePayloads);

    LOG.info("Submitting DAG (submitter={})", submitter);
    // close() may have been called, in which case currentHiveMr3Client.submitDag() raises Exception
    MR3JobRef mr3JobRef = currentHiveMr3Client.submitDag(
        dagProto, addtlAmLocalResources, submitPayloads,
        workMap, dag, ctx, isShutdown);

    synchronized (this) {
      // Do not record resources until MR3 has accepted the submission.
      // Otherwise a failed submission would cause a retry to omit resources which the AM may never have received.
      if (hiveMr3Client == currentHiveMr3Client) {
        commitAmLocalResources(addtlAmLocalResources, addtlLocalResourcePayloads);
      }
    }

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);
    return mr3JobRef;
  }

  private boolean isOpen(HiveMR3Client currentHiveMr3Client) throws Exception {
    return
        (currentHiveMr3Client != null) &&
        (currentHiveMr3Client.getClientState() != MR3ClientState.SHUTDOWN);
  }

  // MR3Conf from createDagConf() is the only MR3Conf passed to MR3 as part of submitting a DAG.
  private MR3Conf createDagConf(Configuration mr3TaskConf, String dagUser, String queryId, JobConf commonJobConf) {
    boolean confStopCrossDagReuse = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_CONTAINER_STOP_CROSS_DAG_REUSE);
    String queueName = HiveConf.getVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_DAG_QUEUE_NAME);
    boolean includeIndeterminateVertex = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_DAG_INCLUDE_INDETERMINATE_VERTEX);
    int taskMaxFailedAttempts = HiveConf.getIntVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_AM_TASK_MAX_FAILED_ATTEMPTS);
    float concurrentRunThreshold = HiveConf.getFloatVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_AM_TASK_CONCURRENT_RUN_THRESHOLD_PERCENT);

    boolean useFreeMemoryWriterOutput = commonJobConf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT_DEFAULT);
    boolean deleteVertexLocalDirectory = useFreeMemoryWriterOutput ||
        HiveConf.getBoolVar(mr3TaskConf, HiveConf.ConfVars.MR3_DAG_DELETE_VERTEX_LOCAL_DIRECTORY);
    if (useFreeMemoryWriterOutput) {
      LOG.info("{}: Setting {} to true because {} is set to true", queryId,
          HiveConf.ConfVars.MR3_DAG_DELETE_VERTEX_LOCAL_DIRECTORY.varname,
          TezRuntimeConfiguration.TEZ_RUNTIME_USE_FREE_MEMORY_WRITER_OUTPUT);
    }

    int maxNumWorkers = HiveConf.getIntVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_CONTAINER_MAX_NUM_WORKERS);
    MR3ConfBuilder confBuilder;
    if (shareMr3Session) {
      // TODO: if HIVE_SERVER2_ENABLE_DOAS is false, sessionUser.equals(dagUser) is always true
      boolean stopCrossDagReuse = sessionUser.equals(dagUser) && confStopCrossDagReuse;
      // do not add sessionConf because Configuration for MR3Session should be reused.
      confBuilder = new MR3ConfBuilder(false)
          .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_STOP_CROSS_DAG_REUSE(), stopCrossDagReuse);
    } else {
      // add mr3TaskConf because this session is for the DAG being submitted.
      confBuilder = new MR3ConfBuilder(false)
          .addResource(mr3TaskConf)
          .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_STOP_CROSS_DAG_REUSE(), confStopCrossDagReuse);
    }
    if (maxNumWorkers < HiveConf.ConfVars.MR3_CONTAINER_MAX_NUM_WORKERS.defaultIntVal) {
      confBuilder.setInt(MR3Conf$.MODULE$.MR3_CONTAINER_MAX_NUM_WORKERS(), maxNumWorkers);
    }
    if (queryId != null) {
      confBuilder.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, queryId);  // from HIVE-23429
    }
    return confBuilder
        .setInt(MR3Conf$.MODULE$.MR3_AM_TASK_MAX_FAILED_ATTEMPTS(), taskMaxFailedAttempts)
        .setDouble(MR3Conf$.MODULE$.MR3_AM_TASK_CONCURRENT_RUN_THRESHOLD_PERCENT(), concurrentRunThreshold)
        .setBoolean(MR3Conf$.MODULE$.MR3_AM_NOTIFY_DESTINATION_VERTEX_COMPLETE(), deleteVertexLocalDirectory)
        .set(MR3Conf$.MODULE$.MR3_DAG_QUEUE_NAME(), queueName)
        .setBoolean(MR3Conf$.MODULE$.MR3_DAG_INCLUDE_INDETERMINATE_VERTEX(), includeIndeterminateVertex)
        .build();
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  /**
   * @param localResources
   * @return Map of AM LocalResources not yet committed to this session
   */
  private Map<String, LocalResource> getAdditionalAmLocalResources(
      Map<String, LocalResource> localResources,
      Map<String, LocalResourcePayload> localResourcePayloads) {
    Preconditions.checkArgument(localResources.keySet().equals(localResourcePayloads.keySet()),
        "AM local resource and payload names must match");
    Map<String, LocalResource> addtlLocalResources = new HashMap<String, LocalResource>();

    for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
      if (!amLocalResources.containsKey(entry.getKey())) {
        addtlLocalResources.put(entry.getKey(), entry.getValue());
      } else {
        Preconditions.checkArgument(amLocalResourceDigests.get(entry.getKey()).equals(
            localResourcePayloads.get(entry.getKey()).digest()),
            "AM local resource name %s is already associated with different content", entry.getKey());
      }
    }

    return addtlLocalResources;
  }

  // MR3 atomically rejects conflicting name/digest associations during submitDag().
  private void commitAmLocalResources(
      Map<String, LocalResource> localResources,
      Map<String, LocalResourcePayload> localResourcePayloads) {
    for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
      LocalResourcePayload payload = localResourcePayloads.get(entry.getKey());
      amLocalResources.putIfAbsent(entry.getKey(), entry.getValue());
      amLocalResourceDigests.putIfAbsent(entry.getKey(), payload.digest());
    }
  }

  private void waitUntilMr3ClientReady() throws Exception {
    long timeoutMs = sessionConf.getTimeVar(
        HiveConf.ConfVars.MR3_CLIENT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    long endTimeoutTimeMs = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < endTimeoutTimeMs) {
      try {
        if (isMr3ClientReady()) {
          return;
        }
      } catch (Exception ex) {
        // Unfortunately We cannot distinguish between 'DAGAppMaster has not started yet' and 'DAGAppMaster
        // has already terminated'. In both cases, we get Exception.
        LOG.info("Exception while waiting for MR3Client state: " + ex.getClass().getSimpleName());
      }
      Thread.sleep(1000);
    }
    throw new Exception("MR3Client failed to start or transition to Ready");
  }

  private boolean isMr3ClientReady() throws Exception {
    assert(hiveMr3Client != null);
    MR3ClientState state = hiveMr3Client.getClientState();
    LOG.info("Current MR3Client state = " + state.toString());
    return state == MR3ClientState.READY;
  }

  public boolean isRunningFromApplicationReport() {
    HiveMR3Client currentHiveMr3Client;
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
    }

    if (currentHiveMr3Client != null) {
      try {
        return currentHiveMr3Client.isRunningFromApplicationReport();
      } catch (Exception ex) {
        return false;
      }
    } else {
      return false;
    }
  }

  public int getEstimateNumTasksOrNodes(int taskMemoryInMb) throws Exception {
    HiveMR3Client currentHiveMr3Client;
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
    }
    return currentHiveMr3Client.getEstimateNumTasksOrNodes(taskMemoryInMb);
  }

  public void sendDaemonMessage(String daemonId, ByteString payload) throws Exception {
    HiveMR3Client currentHiveMr3Client;
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
    }
    currentHiveMr3Client.sendDaemonMessage(daemonId, payload);
  }
}
