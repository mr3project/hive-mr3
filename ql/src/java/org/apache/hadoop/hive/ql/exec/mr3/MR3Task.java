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

package org.apache.hadoop.hive.ql.exec.mr3;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.dag.Edge;
import org.apache.hadoop.hive.ql.exec.mr3.dag.GroupInputEdge;
import org.apache.hadoop.hive.ql.exec.mr3.dag.Vertex;
import org.apache.hadoop.hive.ql.exec.mr3.dag.VertexGroup;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManager;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.app.dag.impl.RootInputVertexManager;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MR3Task handles the execution of TezWork.
 *
 */
public class MR3Task {

  public static final String HIVE_CONF_COMPILE_START_TIME = "hive.conf.compile.start.time";
  public static final String HIVE_CONF_COMPILE_END_TIME = "hive.conf.compile.end.time";

  private static final String CLASS_NAME = MR3Task.class.getName();
  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final Logger LOG = LoggerFactory.getLogger(MR3Task.class);

  private final HiveConf conf;
  private final SessionState.LogHelper console;
  private final AtomicBoolean isShutdown;
  private final DAGUtils dagUtils;

  private TezCounters counters;
  private Throwable exception;

  // updated in setupSubmit()
  private MR3Session mr3Session = null;
  // mr3ScratchDir is always set to a directory on HDFS.
  // we create mr3ScratchDir only if TezWork.configureJobConfAndExtractJars() returns a non-empty list.
  // note that we always need mr3ScratchDir for the path to Map/Reduce Plans.
  private Path mr3ScratchDir = null;
  private boolean mr3ScratchDirCreated = false;
  private Map<String, LocalResource> amDagCommonLocalResources = null;

  public MR3Task(HiveConf conf, SessionState.LogHelper console, AtomicBoolean isShutdown) {
    this.conf = conf;
    this.console = console;
    this.isShutdown = isShutdown;
    this.dagUtils = DAGUtils.getInstance();
    this.exception = null;
  }

  public TezCounters getTezCounters() {
    return counters;
  }

  public Throwable getException() {
    return exception;
  }

  private void setException(Throwable ex) {
    exception = ex;
  }

  public int execute(DriverContext driverContext, TezWork tezWork) {
    int returnCode = 1;   // 1 == error
    boolean cleanContext = false;
    Context context = null;
    MR3JobRef mr3JobRef = null;
    Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

    console.printInfo("MR3Task.execute(): " + tezWork.getName());

    try {
      context = driverContext.getCtx();
      if (context == null) {
        context = new Context(conf);
        cleanContext = true;
      }

      // jobConf holds all the configurations for hadoop, tez, and hive, but not MR3
      // effectful: conf is updated
      JobConf jobConf = dagUtils.createConfiguration(conf);

      DAG dag = setupSubmit(jobConf, tezWork, context, workToConf);

      // 4. submit
      try {
        mr3JobRef = mr3Session.submit(
            dag, amDagCommonLocalResources, conf, tezWork.getWorkMap(), context, isShutdown, perfLogger);
        // mr3Session can be closed at any time, so the call may fail
        // handle only Exception from mr3Session.submit()
      } catch (Exception submitEx) {
        // if mr3Session is alive, return null
        // if mr3Session is not alive, ***close it*** and return a new one
        MR3SessionManager mr3SessionManager = MR3SessionManagerImpl.getInstance();
        MR3Session newMr3Session = mr3SessionManager.triggerCheckApplicationStatus(mr3Session, this.conf);
        if (newMr3Session == null) {
          LOG.warn("Current MR3Session is still valid, failing MR3Task");
          throw submitEx;
        } else {
          // newMr3Session can be closed at any time
          LOG.warn("Current MR3Session is invalid, setting new MR3Session and trying again");
          // mr3Session is already closed by MR3SessionManager
          SessionState.get().setMr3Session(newMr3Session);
          // simulate completing the current call to execute() and calling it again
          // 1. simulate completing the current call to execute()
          Utilities.clearWork(conf);
          // no need to call cleanContextIfNecessary(cleanContext, context)
          if (mr3ScratchDir != null && mr3ScratchDirCreated) {
            dagUtils.cleanMr3Dir(mr3ScratchDir, conf);
          }
          // 2. call again
          DAG newDag = setupSubmit(jobConf, tezWork, context, workToConf);
          // mr3Session can be closed at any time, so the call may fail
          mr3JobRef = mr3Session.submit(
              newDag, amDagCommonLocalResources, conf, tezWork.getWorkMap(), context, isShutdown, perfLogger);
        }
      }

      // 5. monitor
      console.printInfo("Status: Running (Executing on MR3 DAGAppMaster): " + tezWork.getName());
      // for extracting ApplicationID by mr3-run/hive/hive-setup.sh#hive_setup_get_yarn_report_from_file():
      // console.printInfo(
      //     "Status: Running (Executing on MR3 DAGAppMaster with ApplicationID " + mr3JobRef.getJobId() + ")");
      returnCode = mr3JobRef.monitorJob();
      if (returnCode != 0) {
        this.setException(new HiveException(mr3JobRef.getDiagnostics()));
      }

      counters = mr3JobRef.getDagCounters();
      if (LOG.isInfoEnabled() && counters != null
          && (HiveConf.getBoolVar(conf, HiveConf.ConfVars.MR3_EXEC_SUMMARY) ||
          Utilities.isPerfOrAboveLogging(conf))) {
        for (CounterGroup group: counters) {
          LOG.info(group.getDisplayName() + ":");
          for (TezCounter counter: group) {
            LOG.info("   " + counter.getDisplayName() + ": " + counter.getValue());
          }
        }
      }

      LOG.info("MR3Task completed");
    } catch (Exception e) {
      LOG.error("Failed to execute MR3Task", e);
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      this.setException(new HiveException(sw.toString()));
      returnCode = 1;   // indicates failure
    } finally {
      Utilities.clearWork(conf);

      // Clear gWorkMap
      for (BaseWork w : tezWork.getAllWork()) {
        JobConf workCfg = workToConf.get(w);
        if (workCfg != null) {
          Utilities.clearWorkMapForConf(workCfg);
        }
      }

      cleanContextIfNecessary(cleanContext, context);

      // TODO: clean before close()?
      // Make sure tmp files from task can be moved in this.close(tezWork, returnCode).
      if (mr3ScratchDir != null && mr3ScratchDirCreated) {
        dagUtils.cleanMr3Dir(mr3ScratchDir, conf);
      }

      // We know the job has been submitted, should try and close work
      if (mr3JobRef != null) {
        // returnCode will only be overwritten if close errors out
        returnCode = close(tezWork, returnCode);
      }
    }

    return returnCode;
  }

  private DAG setupSubmit(JobConf jobConf, TezWork tezWork, Context context,
                          Map<BaseWork, JobConf> workToConf) throws Exception {
    mr3Session = getMr3Session(conf);
    // mr3Session can be closed at any time
    Path sessionScratchDir = mr3Session.getSessionScratchDir();
    // sessionScratchDir is not null because mr3Session has started:
    //   if shareMr3Session == false, this MR3Task/thread owns mr3Session, which must have started.
    //   if shareMr3Session == true, close() is called only from MR3Session.shutdown() in the end.
    // mr3ScratchDir is created in buildDag() if necessary.

    // 1. read confLocalResources
    // confLocalResource = specific to this MR3Task obtained from conf
    // localizeTempFilesFromConf() updates conf by calling HiveConf.setVar(HIVEADDEDFILES/JARS/ARCHIVES)
    // Note that we should not copy to mr3ScratchDir in order to avoid redundant localization.
    List<LocalResource> confLocalResources = dagUtils.localizeTempFilesFromConf(sessionScratchDir, conf);

    // 2. compute amDagCommonLocalResources
    amDagCommonLocalResources = dagUtils.convertLocalResourceListToMap(confLocalResources);

    // 3. create DAG
    DAG dag = buildDag(jobConf, tezWork, context, amDagCommonLocalResources, sessionScratchDir, workToConf);
    console.printInfo("Finished building DAG, now submitting: " + tezWork.getName());

    if (this.isShutdown.get()) {
      throw new HiveException("Operation cancelled before submit()");
    }

    return dag;
  }

  private void cleanContextIfNecessary(boolean cleanContext, Context context) {
    if (cleanContext) {
      try {
        context.clear();
      } catch (Exception e) {
        LOG.warn("Failed to clean up after MR3 job");
      }
    }
  }

  private MR3Session getMr3Session(HiveConf hiveConf) throws Exception {
    MR3SessionManager mr3SessionManager = MR3SessionManagerImpl.getInstance();

    // TODO: currently hiveConf.getMr3ConfigUpdated() always returns false
    if (hiveConf.getMr3ConfigUpdated() && !mr3SessionManager.getShareMr3Session()) {
      MR3Session mr3Session = SessionState.get().getMr3Session();
      if (mr3Session != null) {
        // this MR3Task/thread owns mr3session, so it must have started
        mr3SessionManager.closeSession(mr3Session);
        SessionState.get().setMr3Session(null);
      }
      hiveConf.setMr3ConfigUpdated(false);
    }

    MR3Session mr3Session = SessionState.get().getMr3Session();
    if (mr3Session == null) {
      console.printInfo("Starting MR3 Session...");
      mr3Session = mr3SessionManager.getSession(hiveConf);
      SessionState.get().setMr3Session(mr3Session);
    }
    // if shareMr3Session == false, this MR3Task/thread owns mr3Session, which must be start.
    // if shareMr3Session == true, close() is called only from MR3Session.shutdown() in the end.
    return mr3Session;
  }

  /**
   * localizes and returns LocalResources for the DAG (inputOutputJars, Hive StorageHandlers)
   * Converts inputOutputJars: String[] to resources: Map<String, LocalResource>
   */
  private Map<String, LocalResource> getDagLocalResources(
      String[] dagJars, Path scratchDir, JobConf jobConf) throws Exception {
    List<LocalResource> localResources = dagUtils.localizeTempFiles(scratchDir, jobConf, dagJars);

    Map<String, LocalResource> resources = dagUtils.convertLocalResourceListToMap(localResources);
    checkInputOutputLocalResources(resources);

    return resources;
  }

  private void checkInputOutputLocalResources(
      Map<String, LocalResource> inputOutputLocalResources) {
    if (LOG.isDebugEnabled()) {
      if (inputOutputLocalResources == null || inputOutputLocalResources.size() == 0) {
        LOG.debug("No local resources for this MR3Task I/O");
      } else {
        for (LocalResource lr: inputOutputLocalResources.values()) {
          LOG.debug("Adding local resource: " + lr.getResource());
        }
      }
    }
  }

  private DAG buildDag(
      JobConf jobConf, TezWork tezWork, Context context,
      Map<String, LocalResource> amDagCommonLocalResources, Path sessionScratchDir,
      Map<BaseWork, JobConf> workToConf) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_BUILD_DAG);
    Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();

    // getAllWork returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = tezWork.getAllWork();
    Collections.reverse(ws);

    // Get all user jars from tezWork (e.g. input format stuff).
    // jobConf updated with "tmpjars" and credentials
    String[] inputOutputJars = tezWork.configureJobConfAndExtractJars(jobConf);

    Map<String, LocalResource> inputOutputLocalResources;
    if (inputOutputJars != null && inputOutputJars.length > 0) {
      // we create mr3ScratchDir to localize inputOutputJars[] to HDFS
      mr3ScratchDir = dagUtils.createMr3ScratchDir(sessionScratchDir, conf, true);
      mr3ScratchDirCreated = true;
      inputOutputLocalResources = getDagLocalResources(inputOutputJars, mr3ScratchDir, jobConf);
      List<String> keysToRemove = new ArrayList();
      for (String lrName : inputOutputLocalResources.keySet()) {
        if (amDagCommonLocalResources.containsKey(lrName)) {
          LOG.info("Skipping LocalResource which is already included: " + lrName);
          keysToRemove.add(lrName);
        }
      }
      for (String key: keysToRemove) {
        inputOutputLocalResources.remove(key);
      }
    } else {
      // no need to create mr3ScratchDir (because DAG Plans are passed via RPC)
      mr3ScratchDir = dagUtils.createMr3ScratchDir(sessionScratchDir, conf, false);
      mr3ScratchDirCreated = false;
      inputOutputLocalResources = new HashMap<String, LocalResource>();
    }

    // the name of the dag is what is displayed in the AM/Job UI
    String dagName = tezWork.getName();
    JSONObject json = new JSONObject().put("context", "Hive").put("description", context.getCmd());
    String dagInfo = json.toString();
    Credentials dagCredentials = jobConf.getCredentials();

    // if doAs == true,
    //   UserGroupInformation.getCurrentUser() == the user from Beeline (auth:PROXY)
    //   UserGroupInformation.getCurrentUser() holds HIVE_DELEGATION_TOKEN
    // if doAs == false,
    //   UserGroupInformation.getCurrentUser() == the user from HiveServer2 (auth:KERBEROS)
    //   UserGroupInformation.getCurrentUser() does not hold HIVE_DELEGATION_TOKEN (which is unnecessary)

    DAG dag = DAG.create(dagName, dagInfo, dagCredentials);
    if (LOG.isDebugEnabled()) {
      LOG.debug("DagInfo: " + dagInfo);
    }

    for (BaseWork w: ws) {
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_CREATE_VERTEX + w.getName());

      if (w instanceof UnionWork) {
        buildVertexGroupEdges(
            dag, tezWork, (UnionWork) w, workToVertex, workToConf);
      } else {
        buildRegularVertexEdge(
            jobConf, dag, tezWork, w, workToVertex, workToConf, mr3ScratchDir, context);
      }

      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_CREATE_VERTEX + w.getName());
    }

    addMissingVertexManagersToDagVertices(jobConf, dag);

    // add input/output LocalResources and amDagLocalResources, and then add paths to DAG credentials

    dag.addLocalResources(inputOutputLocalResources.values());
    dag.addLocalResources(amDagCommonLocalResources.values());

    if (dagUtils.shouldAddPathsToCredentials(jobConf)) {
      LOG.info("Adding credentials for DAG: " + dagName);
      Set<Path> allPaths = new HashSet<Path>();
      for (LocalResource lr: inputOutputLocalResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      for (LocalResource lr: amDagCommonLocalResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      for (Path path: allPaths) {
        LOG.info("Marking Path as needing credentials for DAG: " + path);
      }
      final String[] additionalCredentialsSource = HiveConf.getTrimmedStringsVar(jobConf,
          HiveConf.ConfVars.MR3_DAG_ADDITIONAL_CREDENTIALS_SOURCE);
      for (String addPath: additionalCredentialsSource) {
        try {
          allPaths.add(new Path(addPath));
          LOG.info("Additional source for DAG credentials: " + addPath);
        } catch (IllegalArgumentException ex) {
          LOG.error("Ignoring a wrong path for DAG credentials: " + addPath);
        }
      }
      dag.addPathsToCredentials(dagUtils, allPaths, jobConf);
    } else {
      LOG.info("Skip adding credentials for DAG: " + dagName);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_BUILD_DAG);
    return dag;
  }

  private void buildVertexGroupEdges(
      DAG dag, TezWork tezWork, UnionWork unionWork,
      Map<BaseWork, Vertex> workToVertex,
      Map<BaseWork, JobConf> workToConf) throws IOException {
    List<BaseWork> unionWorkItems = new LinkedList<BaseWork>();
    List<BaseWork> children = new LinkedList<BaseWork>();

    // split the children into vertices that make up the union and vertices that are
    // proper children of the union
    for (BaseWork v: tezWork.getChildren(unionWork)) {
      TezEdgeProperty.EdgeType type = tezWork.getEdgeProperty(unionWork, v).getEdgeType();
      if (type == TezEdgeProperty.EdgeType.CONTAINS) {
        unionWorkItems.add(v);
      } else {
        children.add(v);
      }
    }

    // VertexGroup.name == unionWork.getName()
    // VertexGroup.outputs == (empty)
    // VertexGroup.members
    Vertex[] members = new Vertex[unionWorkItems.size()];
    int i = 0;
    for (BaseWork v: unionWorkItems) {
      members[i++] = workToVertex.get(v);
    }

    // VertexGroup.edges
    // All destVertexes use the same Key-class, Val-class and partitioner.
    // Pick any member vertex to figure out the Edge configuration.
    JobConf parentConf = workToConf.get(unionWorkItems.get(0));
    checkOutputSpec(unionWork, parentConf);

    List<GroupInputEdge> edges = new ArrayList<GroupInputEdge>();
    for (BaseWork v: children) {
      GroupInputEdge edge = dagUtils.createGroupInputEdge(
          parentConf, workToVertex.get(v),
          tezWork.getEdgeProperty(unionWork, v), v, tezWork);
      edges.add(edge);
    }

    VertexGroup vertexGroup = new VertexGroup(unionWork.getName(), members, edges, null);
    dag.addVertexGroup(vertexGroup);
  }

  private void buildRegularVertexEdge(
      JobConf jobConf,
      DAG dag, TezWork tezWork, BaseWork baseWork,
      Map<BaseWork, Vertex> workToVertex,
      Map<BaseWork, JobConf> workToConf,
      Path mr3ScratchDir,
      Context context) throws Exception {
    JobConf vertexJobConf = dagUtils.initializeVertexConf(jobConf, context, baseWork);
    checkOutputSpec(baseWork, vertexJobConf);
    TezWork.VertexType vertexType = tezWork.getVertexType(baseWork);
    boolean isFinal = tezWork.getLeaves().contains(baseWork);

    // update vertexJobConf before calling createVertex() which calls createBy
    int numChildren = tezWork.getChildren(baseWork).size();
    if (numChildren > 1) {  // added from HIVE-22744
      String value = vertexJobConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
      int originalValue = 0;
      if(value == null) {
        originalValue = TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT;
      } else {
        originalValue = Integer.valueOf(value);
      }
      int newValue = (int) (originalValue / numChildren);
      vertexJobConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, Integer.toString(newValue));
      LOG.info("Modified " + TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB + " to " + newValue);
    }

    Vertex vertex = dagUtils.createVertex(vertexJobConf, baseWork, mr3ScratchDir, isFinal, vertexType, tezWork);
    dag.addVertex(vertex);

    if (dagUtils.shouldAddPathsToCredentials(jobConf)) {
      LOG.info("Adding credentials for paths: " + baseWork.getName());
      Set<Path> paths = dagUtils.getPathsForCredentials(baseWork);
      if (!paths.isEmpty()) {
        dag.addPathsToCredentials(dagUtils, paths, jobConf);
      }
    } else {
      LOG.info("Skip adding credentials for paths: " + baseWork.getName());
    }

    workToVertex.put(baseWork, vertex);
    workToConf.put(baseWork, vertexJobConf);

    // add all dependencies (i.e.: edges) to the graph
    for (BaseWork v: tezWork.getChildren(baseWork)) {
      assert workToVertex.containsKey(v);
      TezEdgeProperty edgeProp = tezWork.getEdgeProperty(baseWork, v);
      Edge e = dagUtils.createEdge(
          vertexJobConf, vertex, workToVertex.get(v), edgeProp, v, tezWork);
      dag.addEdge(e);
    }
  }

  private void checkOutputSpec(BaseWork work, JobConf jc) throws IOException {
    for (Operator<?> op : work.getAllOperators()) {
      if (op instanceof FileSinkOperator) {
        ((FileSinkOperator) op).checkOutputSpecs(null, jc);
      }
    }
  }

   /**
    * MR3 Requires all Vertices to have VertexManagers, the current impl. will produce Vertices
    * missing VertexManagers. Post-processes Dag to add missing VertexManagers.
    * @param dag
    * @throws Exception
    */
  private void addMissingVertexManagersToDagVertices(JobConf jobConf, DAG dag) throws Exception {
    // ByteString is immutable, so can be safely shared
    Configuration pluginConfRootInputVertexManager = createPluginConfRootInputVertexManager(jobConf);
    ByteString userPayloadRootInputVertexManager =
        org.apache.tez.common.TezUtils.createByteStringFromConf(pluginConfRootInputVertexManager);

    // TODO: unnecessary if jobConf.getBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM) == false
    Configuration pluginConfShuffleVertexManagerAuto =
        dagUtils.createPluginConfShuffleVertexManagerAutoParallel(jobConf);
    dagUtils.setupMinMaxSrcFraction(jobConf, pluginConfShuffleVertexManagerAuto);
    ByteString userPayloadShuffleVertexManagerAuto =
        org.apache.tez.common.TezUtils.createByteStringFromConf(pluginConfShuffleVertexManagerAuto);

    Configuration pluginConfShuffleVertexManagerFixed =
        dagUtils.createPluginConfShuffleVertexManagerFixed(jobConf);
    dagUtils.setupMinMaxSrcFraction(jobConf, pluginConfShuffleVertexManagerFixed);
    ByteString userPayloadShuffleVertexManagerFixed =
        org.apache.tez.common.TezUtils.createByteStringFromConf(pluginConfShuffleVertexManagerFixed);

    for (Vertex vertex : dag.getVertices().values()) {
      if (vertex.getVertexManagerPlugin() == null) {
        vertex.setVertexManagerPlugin(dagUtils.getVertexManagerForVertex(
            vertex, userPayloadRootInputVertexManager, userPayloadShuffleVertexManagerAuto, userPayloadShuffleVertexManagerFixed));
      }
    }
  }

  private Configuration createPluginConfRootInputVertexManager(JobConf jobConf) {
    Configuration pluginConf = new Configuration(false);

    boolean slowStartEnabled = jobConf.getBoolean(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START,
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT);
    pluginConf.setBoolean(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START, slowStartEnabled);

    float slowStartMinFraction = jobConf.getFloat(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION,
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    pluginConf.setFloat(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION, slowStartMinFraction);

    float slowStartMaxFraction = jobConf.getFloat(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION,
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);
    pluginConf.setFloat(
        RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION, slowStartMaxFraction);

    return pluginConf;
  }

  /*
   * close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  private int close(TezWork tezWork, int returnCode) {
    try {
      List<BaseWork> ws = tezWork.getAllWork();
      for (BaseWork w: ws) {
        if (w instanceof MergeJoinWork) {
          w = ((MergeJoinWork) w).getMainWork();
        }
        for (Operator<?> op: w.getAllOperators()) {
          op.jobClose(conf, returnCode == 0);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (returnCode == 0) {
        returnCode = 3;
        String mesg = "Job Commit failed with exception '"
                + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    return returnCode;
  }
}
