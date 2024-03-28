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

import com.google.common.base.Preconditions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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
  // read in submit() via updateAmCredentials()
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
  private Map<String, LocalResource> amLocalResources = new HashMap<String, LocalResource>();
  // via updateAmCredentials()
  private Credentials amCredentials;

  // private List<LocalResource> amDagCommonLocalResources = new ArrayList<LocalResource>();

  DAGUtils dagUtils = DAGUtils.getInstance();

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

  public synchronized void connect(HiveConf conf, ApplicationId appId) throws HiveException {
    this.sessionConf = conf;
    try {
      setupHiveMr3Client(conf);

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

  private void setupHiveMr3Client(HiveConf conf) throws Exception {
    sessionScratchDir = createSessionScratchDir(sessionId);
    setAmStagingDir(sessionScratchDir);

    // 1. read hiveJarLocalResources

    // getSessionInitJars() returns hive-exec.jar + HIVEAUXJARS
    List<LocalResource> hiveJarLocalResources =
        dagUtils.localizeTempFiles(sessionScratchDir, conf, dagUtils.getSessionInitJars(conf));
    Map<String, LocalResource> additionalSessionLocalResources =
        dagUtils.convertLocalResourceListToMap(hiveJarLocalResources);

    Credentials additionalSessionCredentials = null;  // null okay because it is passed to scala Option()
    if (dagUtils.shouldAddPathsToCredentials(conf)) {
      additionalSessionCredentials = new Credentials();
      Set<Path> allPaths = new HashSet<Path>();
      for (LocalResource lr: additionalSessionLocalResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      dagUtils.addPathsToCredentials(additionalSessionCredentials, allPaths, conf);
    }

    // 2. read confLocalResources

    // confLocalResource = specific to this MR3Session obtained from sessionConf
    // localizeTempFilesFromConf() updates sessionConf by calling HiveConf.setVar(HIVEADDEDFILES/JARS/ARCHIVES)
    List<LocalResource> confLocalResources = dagUtils.localizeTempFilesFromConf(sessionScratchDir, conf);

    // We do not add confLocalResources to additionalSessionLocalResources because
    // dagUtils.localizeTempFilesFromConf() will be called each time a new DAG is submitted.

    // 3. set initAmLocalResources

    List<LocalResource> initAmLocalResources = new ArrayList<LocalResource>();
    initAmLocalResources.addAll(confLocalResources);
    Map<String, LocalResource> initAmLocalResourcesMap =
        dagUtils.convertLocalResourceListToMap(initAmLocalResources);

    // 4. update amLocalResource and create HiveMR3Client

    updateAmLocalResources(initAmLocalResourcesMap);
    updateAmCredentials(initAmLocalResourcesMap, conf);

    LOG.info("Creating HiveMR3Client (id: " + sessionId + ", scratch dir: " + sessionScratchDir + ")");
    hiveMr3Client = HiveMR3ClientFactory.createHiveMr3Client(
        sessionId,
        amCredentials, amLocalResources,
        additionalSessionCredentials, additionalSessionLocalResources,
        conf);
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
    Utilities.createDirsWithPermission(
        sessionConf, mr3SessionScratchDir, new FsPermission(SessionState.SESSION_SCRATCH_DIR_PERMISSION), true);
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

    amCredentials = null;

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
      Configuration mr3TaskConf,
      Map<String, BaseWork> workMap,
      Context ctx,
      AtomicBoolean isShutdown,
      PerfLogger perfLogger) throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);

    HiveMR3Client currentHiveMr3Client;
    Map<String, LocalResource> addtlAmLocalResources = null;
    Credentials addtlAmCredentials = null;
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
      if (currentHiveMr3Client != null) {
        // close() has not been called
        addtlAmLocalResources = updateAmLocalResources(newAmLocalResources);
        addtlAmCredentials = updateAmCredentials(newAmLocalResources, mr3TaskConf);
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
    MR3Conf dagConf = createDagConf(mr3TaskConf, dagUser);

    // sessionConf is not passed to MR3; only dagConf is passed to MR3 as a component of DAGProto.dagConf.
    DAGAPI.DAGProto dagProto = dag.createDagProto(mr3TaskConf, dagConf);

    LOG.info("Submitting DAG");
    // close() may have been called, in which case currentHiveMr3Client.submitDag() raises Exception
    MR3JobRef mr3JobRef = currentHiveMr3Client.submitDag(
        dagProto, addtlAmCredentials, addtlAmLocalResources, workMap, dag, ctx, isShutdown);

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);
    return mr3JobRef;
  }

  private boolean isOpen(HiveMR3Client currentHiveMr3Client) throws Exception {
    return
        (currentHiveMr3Client != null) &&
        (currentHiveMr3Client.getClientState() != MR3ClientState.SHUTDOWN);
  }

  // MR3Conf from createDagConf() is the only MR3Conf passed to MR3 as part of submitting a DAG.
  private MR3Conf createDagConf(Configuration mr3TaskConf, String dagUser) {
    boolean confStopCrossDagReuse = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_CONTAINER_STOP_CROSS_DAG_REUSE);
    String queueName = HiveConf.getVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_DAG_QUEUE_NAME);
    boolean includeIndeterminateVertex = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_DAG_INCLUDE_INDETERMINATE_VERTEX);
    int taskMaxFailedAttempts = HiveConf.getIntVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_AM_TASK_MAX_FAILED_ATTEMPTS);
    int concurrentRunThreshold = HiveConf.getIntVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_AM_TASK_CONCURRENT_RUN_THRESHOLD_PERCENT);
    boolean deleteVertexLocalDirectory = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_DAG_DELETE_VERTEX_LOCAL_DIRECTORY);
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
    return confBuilder
        .setInt(MR3Conf$.MODULE$.MR3_AM_TASK_MAX_FAILED_ATTEMPTS(), taskMaxFailedAttempts)
        .setInt(MR3Conf$.MODULE$.MR3_AM_TASK_CONCURRENT_RUN_THRESHOLD_PERCENT(), concurrentRunThreshold)
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
   * @return Map of newly added AM LocalResources
   */
  private Map<String, LocalResource> updateAmLocalResources(
      Map<String, LocalResource> localResources ) {
    Map<String, LocalResource> addtlLocalResources = new HashMap<String, LocalResource>();

    for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
      if (!amLocalResources.containsKey(entry.getKey())) {
        amLocalResources.put(entry.getKey(), entry.getValue());
        addtlLocalResources.put(entry.getKey(), entry.getValue());
      }
    }

    return addtlLocalResources;
  }

  /**
   * @param localResources to be added to Credentials
   * @return returns Credentials for newly added LocalResources only
   */
  private Credentials updateAmCredentials(
      Map<String, LocalResource> localResources, Configuration conf) throws Exception {
    if (amCredentials == null) {
      amCredentials = new Credentials();
    }

    Credentials addtlAmCredentials = new Credentials();

    if (dagUtils.shouldAddPathsToCredentials(conf)) {
      Set<Path> allPaths = new HashSet<Path>();
      for (LocalResource lr: localResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      dagUtils.addPathsToCredentials(addtlAmCredentials, allPaths, sessionConf);

      // hadoop-1 version of Credentials doesn't have method mergeAll()
      // See Jira HIVE-6915 and HIVE-8782
      // TODO: use ShimLoader.getHadoopShims().mergeCredentials(jobConf, addtlJobConf)
      amCredentials.addAll(addtlAmCredentials);
    }

    return addtlAmCredentials;
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
}
