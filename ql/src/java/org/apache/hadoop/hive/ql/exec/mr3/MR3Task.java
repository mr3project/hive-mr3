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

import com.datamonad.mr3.api.LocalResourcePayload;
import com.datamonad.mr3.api.client.DAGStatus;
import com.datamonad.mr3.api.client.VertexStatus;
import com.datamonad.mr3.api.common.MR3Exception;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
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
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.app.dag.impl.RootInputVertexManager;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import scala.Tuple2;
import scala.collection.Iterator;

import static org.apache.hadoop.hive.ql.exec.tez.TezTask.JOB_ID_TEMPLATE;
import static org.apache.hadoop.hive.ql.exec.tez.TezTask.ICEBERG_PROPERTY_PREFIX;
import static org.apache.hadoop.hive.ql.exec.tez.TezTask.ICEBERG_SERIALIZED_TABLE_PREFIX;

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
  private String dagIdStr;
  private String terminalDagStatus;
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
  private Map<String, LocalResourcePayload> amDagCommonLocalResourcePayloads = null;

  private Map<BaseWork, Vertex> workToVertex = null;
  private Map<BaseWork, JobConf> workToConf = null;
  private final Map<String, QueryResultMaterializationContext> queryResultMaterializationContexts = new LinkedHashMap<>();

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

  public String getDagIdStr() {
    return dagIdStr;
  }

  public String getTerminalDagStatus() {
    return terminalDagStatus;
  }

  private void updateDagId(MR3JobRef mr3JobRef) {
    try {
      dagIdStr = mr3JobRef.getDagIdStr();
    } catch (MR3Exception e) {
      LOG.warn("DAG ID is not available: {}", e.getMessage());
    }
  }

  private void setTerminalDagStatus(int returnCode) {
    switch (returnCode) {
    case 0:
      terminalDagStatus = "SUCCEEDED";
      break;
    case 1:
      terminalDagStatus = "KILLED";
      break;
    case 2:
    default:
      terminalDagStatus = "FAILED";
      break;
    }
  }

  public int execute(Context contextFromTezTask, TezWork tezWork) {
    int returnCode = 1;   // 1 == error
    boolean cleanContext = false;
    Context context = null;
    MR3JobRef mr3JobRef = null;
    Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

    console.printInfo("MR3Task.execute(): " + tezWork.getName());

    try {
      context = contextFromTezTask;
      if (context == null) {
        context = new Context(conf);
        cleanContext = true;
      }

      // jobConf holds all the configurations for hadoop, tez, and hive, but not MR3
      JobConf jobConf = dagUtils.createConfiguration(conf);

      DAG dag = setupSubmit(jobConf, tezWork, context, workToConf, false);

      // 4. submit
      try {
        mr3JobRef = mr3Session.submit(
            dag, amDagCommonLocalResources, amDagCommonLocalResourcePayloads,
            conf, tezWork.getWorkMap(), context, isShutdown, perfLogger);
        updateDagId(mr3JobRef);
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
          // 2. call again.
          DAG newDag = setupSubmit(jobConf, tezWork, context, workToConf, true);
          // mr3Session can be closed at any time, so the call may fail
          mr3JobRef = mr3Session.submit(
              newDag, amDagCommonLocalResources, amDagCommonLocalResourcePayloads,
              conf, tezWork.getWorkMap(), context, isShutdown, perfLogger);
          updateDagId(mr3JobRef);
        }
      }

      // 5. monitor
      console.printInfo("Status: Running (Executing on MR3 DAGAppMaster): " + tezWork.getName());
      // for extracting ApplicationID by mr3-run/hive/hive-setup.sh#hive_setup_get_yarn_report_from_file():
      // console.printInfo(
      //     "Status: Running (Executing on MR3 DAGAppMaster with ApplicationID " + mr3JobRef.getJobId() + ")");
      returnCode = mr3JobRef.monitorJob();
      setTerminalDagStatus(returnCode);
      if (returnCode != 0) {
        this.setException(new HiveException(mr3JobRef.getDiagnostics()));
      }

      counters = mr3JobRef.getDagCounters();
      if (LOG.isInfoEnabled() && counters != null
          && (HiveConf.getBoolVar(conf, HiveConf.ConfVars.MR3_EXEC_SUMMARY) ||
          Utilities.isPerfOrAboveLogging(conf))) {
        for (CounterGroup group: counters) {
          LOG.info("{}:", group.getDisplayName());
          for (TezCounter counter: group) {
            LOG.info("   {}: {}", counter.getDisplayName(), counter.getValue());
          }
        }
      }

      // save useful commit information into query state, e.g. for custom commit hooks, like Iceberg
      if (returnCode == 0) {
        DAGStatus dagStatus = mr3JobRef.getDagStatus();
        if (dagStatus == null) {
          throw new MR3Exception("DAGStatus not available with return code == 0");
        }
        String dagIdStr = mr3JobRef.getDagIdStr();    // may throw MR3Exception
        collectCommitInformation(tezWork, dagStatus, dagIdStr);
        collectDagOutputs(dagStatus, context);
        mr3Session.setAlreadyExecutedAnyDag();
      }

      LOG.info("MR3Task completed");
    } catch (Exception e) {
      LOG.error("Failed to execute MR3Task", e);
      if (terminalDagStatus == null) {
        terminalDagStatus = "FAILED";
      }
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

  private void collectCommitInformation(TezWork work, DAGStatus dagStatus, String dagIdStr) {
    for (BaseWork w : work.getAllWork()) {
      JobConf jobConf = workToConf.get(w);
      Vertex vertex = workToVertex.get(w);
      boolean hasIcebergCommitter = Optional.ofNullable(jobConf).map(JobConf::getOutputCommitter)
          .map(Object::getClass).map(Class::getName)
          .filter(name -> name.endsWith("HiveIcebergNoJobCommitter")).isPresent();
      // we should only consider jobs with Iceberg output committer and a data sink
      if (hasIcebergCommitter && !vertex.getDataSinks().isEmpty()) {
        VertexStatus vertexStatus = dagStatus.vertexStatusMap().apply(vertex.getName());
        String[] jobIdParts = dagIdStr.split("_");
        // dagIdStr returns something like: dag_1660836356025_0465_1
        int vertexId = vertexStatus.vertexIdId();
        String jobId = String.format(JOB_ID_TEMPLATE, jobIdParts[1], vertexId, jobIdParts[2]);

        List<String> tables = new ArrayList<>();
        Map<String, String> icebergProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : jobConf) {
          if (entry.getKey().startsWith(ICEBERG_SERIALIZED_TABLE_PREFIX)) {
            // get all target tables this vertex wrote to
            tables.add(entry.getKey().substring(ICEBERG_SERIALIZED_TABLE_PREFIX.length()));
          } else if (entry.getKey().startsWith(ICEBERG_PROPERTY_PREFIX)) {
            // find iceberg props in jobConf as they can be needed, but not available, during job commit
            icebergProperties.put(entry.getKey(), entry.getValue());
          }
        }

        // save information for each target table
        tables.forEach(table -> SessionStateUtil.addCommitInfo(jobConf, table, jobId,
            vertexStatus.progress().numSucceededTasks(), icebergProperties));
      }
    }
  }

  private DAG setupSubmit(JobConf jobConf, TezWork tezWork, Context context,
                          Map<BaseWork, JobConf> workToConf, boolean isSubmissionRetry) throws Exception {
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
    amDagCommonLocalResourcePayloads = new HashMap<>();
    Map<String, LocalResource> confLocalResources = dagUtils.createInlineLocalResourcesFromConf(
        conf, amDagCommonLocalResourcePayloads);

    // 2. compute amDagCommonLocalResources
    amDagCommonLocalResources = confLocalResources;

    // 3. create DAG
    DAG dag = buildDag(
        jobConf, tezWork, context, amDagCommonLocalResources, sessionScratchDir, workToConf, isSubmissionRetry);
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
      String[] dagJars, JobConf jobConf, Map<String, LocalResourcePayload> contentsByName) throws Exception {
    Map<String, LocalResource> resources =
        dagUtils.createInlineLocalResources(jobConf, dagJars, contentsByName);
    checkInputOutputLocalResources(resources);

    return resources;
  }

  private void checkInputOutputLocalResources(
      Map<String, LocalResource> inputOutputLocalResources) {
    if (LOG.isDebugEnabled()) {
      if (inputOutputLocalResources == null || inputOutputLocalResources.isEmpty()) {
        LOG.debug("No local resources for this MR3Task I/O");
      } else {
        for (LocalResource lr: inputOutputLocalResources.values()) {
          LOG.debug("Adding local resource: {}", lr.getResource());
        }
      }
    }
  }

  private DAG buildDag(
      JobConf jobConf, TezWork tezWork, Context context,
      Map<String, LocalResource> amDagCommonLocalResources, Path sessionScratchDir,
      Map<BaseWork, JobConf> workToConf, boolean isSubmissionRetry) throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.MR3_BUILD_DAG);
    Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();

    // getAllWork() returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = tezWork.getAllWork();
    Collections.reverse(ws);

    // Get all user jars from tezWork (e.g. input format stuff).
    // jobConf updated with "tmpjars" and credentials
    String[] inputOutputJars = tezWork.configureJobConfAndExtractJars(jobConf);

    Map<String, LocalResource> inputOutputLocalResources;
    Map<String, LocalResourcePayload> inputOutputLocalResourcePayloads = new HashMap<>();
    if (inputOutputJars != null && inputOutputJars.length > 0) {
      mr3ScratchDir = dagUtils.createMr3ScratchDir(sessionScratchDir, conf, false);
      mr3ScratchDirCreated = false;
      inputOutputLocalResources = getDagLocalResources(
          inputOutputJars, jobConf, inputOutputLocalResourcePayloads);
      List<String> keysToRemove = new ArrayList();
      for (String lrName : inputOutputLocalResources.keySet()) {
        if (amDagCommonLocalResources.containsKey(lrName)) {
          Preconditions.checkArgument(
              inputOutputLocalResourcePayloads.get(lrName).digest().equals(
                  amDagCommonLocalResourcePayloads.get(lrName).digest()),
              "Local resource name %s is associated with conflicting contents", lrName);
          LOG.info("Skipping LocalResource which is already included: {}", lrName);
          keysToRemove.add(lrName);
        }
      }
      for (String key: keysToRemove) {
        inputOutputLocalResources.remove(key);
        inputOutputLocalResourcePayloads.remove(key);
      }
    } else {
      // no need to create mr3ScratchDir (because DAG Plans are passed via RPC)
      mr3ScratchDir = dagUtils.createMr3ScratchDir(sessionScratchDir, conf, false);
      mr3ScratchDirCreated = false;
      inputOutputLocalResources = new HashMap<String, LocalResource>();
    }

    // the name of the dag is what is displayed in the AM/Job UI
    String dagName = tezWork.getName();
    String dagInfo = context.getCmd();

    // vertex name -> JSONObject OperatorGraph: ["vertexMap" --> [vertex name -> VertexOperatorGraph]]
    // Invariant: OperatorGraph.vertexMap[] is defined only on 'vertex name'.
    Map<String, JSONObject> operatorGraphMap = buildOperatorGraphMap(tezWork);

    Credentials dagCredentials = jobConf.getCredentials();
    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);

    // if doAs == true,
    //   UserGroupInformation.getCurrentUser() == the user from Beeline (auth:PROXY)
    //   UserGroupInformation.getCurrentUser() holds HIVE_DELEGATION_TOKEN
    // if doAs == false,
    //   UserGroupInformation.getCurrentUser() == the user from HiveServer2 (auth:KERBEROS)
    //   UserGroupInformation.getCurrentUser() does not hold HIVE_DELEGATION_TOKEN (which is unnecessary)

    DAG dag = DAG.create(dagName, dagInfo, operatorGraphMap, dagCredentials, queryId, jobConf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("DagInfo: {}", dagInfo);
    }

    for (BaseWork w: ws) {
      perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.MR3_CREATE_VERTEX + w.getName());
      if (w instanceof UnionWork) {
        buildVertexGroupEdges(dag, tezWork, (UnionWork) w, workToVertex, workToConf);
      } else {
        buildRegularVertexEdge(
            jobConf, dag, tezWork, w, workToVertex, workToConf, mr3ScratchDir, isSubmissionRetry);
      }
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.MR3_CREATE_VERTEX + w.getName());
    }

    addMissingVertexManagersToDagVertices(jobConf, dag);

    // add input/output LocalResources and amDagLocalResources, and then add paths to DAG credentials

    dag.addLocalResources(inputOutputLocalResources, inputOutputLocalResourcePayloads, true);
    dag.addLocalResources(amDagCommonLocalResources, amDagCommonLocalResourcePayloads, false);

    if (dagUtils.shouldAddPathsToCredentials(jobConf)) {
      LOG.info("Adding credentials for DAG: {}", dagName);
      Set<Path> allPaths = new HashSet<Path>();
      final String[] additionalCredentialsSource = HiveConf.getTrimmedStringsVar(jobConf,
          HiveConf.ConfVars.MR3_DAG_ADDITIONAL_CREDENTIALS_SOURCE);
      for (String addPath: additionalCredentialsSource) {
        try {
          allPaths.add(new Path(addPath));
          LOG.info("Additional source for DAG credentials: {}", addPath);
        } catch (IllegalArgumentException ex) {
          LOG.error("Ignoring a wrong path for DAG credentials: {}", addPath);
        }
      }
      dag.addPathsToCredentials(dagUtils, allPaths, jobConf);
    } else {
      LOG.info("Skip adding credentials for DAG: {}", dagName);
    }

    this.workToVertex = workToVertex;
    this.workToConf = workToConf;

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.MR3_BUILD_DAG);
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
    JobConf parentJobConf = workToConf.get(unionWorkItems.get(0));
    checkOutputSpec(unionWork, parentJobConf);

    List<GroupInputEdge> edges = new ArrayList<GroupInputEdge>();
    for (BaseWork v: children) {
      GroupInputEdge edge = dagUtils.createGroupInputEdge(
          parentJobConf, dag.getCommonJobConf(), workToVertex.get(v),
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
      boolean isSubmissionRetry) throws Exception {
    JobConf vertexJobConf = dagUtils.initializeVertexConf(jobConf, baseWork);
    checkOutputSpec(baseWork, vertexJobConf);
    TezWork.VertexType vertexType = tezWork.getVertexType(baseWork);
    boolean isFinal = tezWork.getLeaves().contains(baseWork);

    enableQueryResultDagOutputModeIfNeeded(baseWork, isFinal, vertexJobConf, isSubmissionRetry);

    // update vertexJobConf before calling createVertex() which calls createBy
    int numChildren = tezWork.getChildren(baseWork).size();
    if (numChildren > 1) {  // added from HIVE-22744
      String value = vertexJobConf.get(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
      int originalValue;
      if(value == null) {
        originalValue = TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT;
      } else {
        originalValue = Integer.parseInt(value);
      }
      int newValue = (int) (originalValue / numChildren);
      vertexJobConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, Integer.toString(newValue));
      LOG.info("Modified {} to {}", TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, newValue);
    }

    Vertex vertex = dagUtils.createVertex(
        vertexJobConf, jobConf, baseWork, mr3ScratchDir, isFinal, vertexType, tezWork);
    dag.addVertex(vertex);

    if (dagUtils.shouldAddPathsToCredentials(jobConf)) {
      LOG.info("Adding credentials for paths: {}", baseWork.getName());
      Set<Path> paths = dagUtils.getPathsForCredentials(baseWork);
      if (!paths.isEmpty()) {
        dag.addPathsToCredentials(dagUtils, paths, jobConf);
      }
    } else {
      LOG.info("Skip adding credentials for paths: {}", baseWork.getName());
    }

    workToVertex.put(baseWork, vertex);
    workToConf.put(baseWork, vertexJobConf);

    // add all dependencies (i.e., edges) to the graph
    for (BaseWork v: tezWork.getChildren(baseWork)) {
      assert workToVertex.containsKey(v);
      TezEdgeProperty edgeProp = tezWork.getEdgeProperty(baseWork, v);
      Edge e = dagUtils.createEdge(
          vertexJobConf, jobConf, vertex, workToVertex.get(v), edgeProp, v, tezWork);
      dag.addEdge(e);
    }
  }

  private void enableQueryResultDagOutputModeIfNeeded(
      BaseWork baseWork, boolean isFinal, JobConf vertexJobConf, boolean isSubmissionRetry) {
    if (!isFinal || SessionState.get() == null || !SessionState.get().isHiveServerQuery()) {
      return;
    }
    for (Operator<?> op : baseWork.getAllOperators()) {
      if (op instanceof FileSinkOperator) {
        FileSinkOperator fsOp = (FileSinkOperator) op;
        FileSinkDesc desc = fsOp.getConf();
        if (isEligibleQueryResultSink(desc)) {
          String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
          String resultId = queryId + "_" + baseWork.getName() + "_" + fsOp.getIdentifier();
          QueryResultMaterializationContext ctx = new QueryResultMaterializationContext(resultId, desc);
          enableQueryResultDagOutputMode(fsOp, ctx, isSubmissionRetry,
              HiveConf.getLongVar(vertexJobConf, HiveConf.ConfVars.HIVE_MR3_QUERY_RESULT_TASK_MAX_BYTES));
        } else {
          // if desc.isMr3QueryResultLocal() == true, we must enable DAG-output mode and thus cannot reach here
          assert !desc.isMr3QueryResultLocal();
        }
      }
    }
  }

  private boolean isEligibleQueryResultSink(FileSinkDesc desc) {
    return desc != null
        && desc.isHiveServerQuery()
        && desc.getIsQuery()
        && !desc.isMmCtas()
        && !desc.isCTASorCM()
        && !desc.getInsertOverwrite()
        && !desc.isDirectInsert()
        && desc.getDynPartCtx() == null
        && !desc.isGatherStats()
        && !desc.isMerge()
        && desc.getWriteType() == org.apache.hadoop.hive.ql.io.AcidUtils.Operation.NOT_ACID
        && desc.getAcidOperation() == null;
  }

  private void enableQueryResultDagOutputMode(
      FileSinkOperator fsOp, QueryResultMaterializationContext ctx, boolean isSubmissionRetry, long maxBytes) {
    FileSinkDesc desc = fsOp.getConf();

    // emitQueryResultToDag = how the MR3 worker should transport result rows to HiveServer2 (i.e., DAG-mode or not)
    // mr3QueryResultLocal = where HiveServer2 should materialize the collected result artifact
    // (emitQueryResultToDag, mr3QueryResultLocal) = (false, true) is an invalid state.

    if (desc.isEmitQueryResultToDag()) {
      assert isSubmissionRetry;   // desc.setEmitQueryResultToDag(true) was called in the first try
      assert ctx.resultId.equals(desc.getQueryResultId());
      assert queryResultMaterializationContexts.containsKey(ctx.resultId);
      return;
    }
    assert !desc.isEmitQueryResultToDag();
    desc.setEmitQueryResultToDag(true);
    desc.setQueryResultId(ctx.resultId);
    desc.setQueryResultMaxBytes(maxBytes);
    registerMaterializationContextIfAbsent(ctx);
  }

  private void registerMaterializationContextIfAbsent(QueryResultMaterializationContext ctx) {
    queryResultMaterializationContexts.putIfAbsent(ctx.resultId, ctx);
  }

  private void collectDagOutputs(DAGStatus dagStatus, Context context) throws Exception {
    if (queryResultMaterializationContexts.isEmpty()) {
      return;
    }
    Map<String, List<ByteString>> outputsById = getDagOutputsById(dagStatus);
    for (QueryResultMaterializationContext ctx : queryResultMaterializationContexts.values()) {
      List<ByteString> outputs = outputsById.get(ctx.resultId);
      materializeQueryResult(ctx, outputs == null ? Collections.emptyList() : outputs, context);
    }
  }

  private Map<String, List<ByteString>> getDagOutputsById(DAGStatus dagStatus) {
    Map<String, List<ByteString>> result = new HashMap<>();
    Iterator<Tuple2<String, ByteString>> outputs = dagStatus.dagOutputs().iterator();
    while (outputs.hasNext()) {
      Tuple2<String, ByteString> output = outputs.next();
      String id = output._1();
      if (queryResultMaterializationContexts.containsKey(id)) {
        result.computeIfAbsent(id, ignored -> new ArrayList<>()).add(output._2());
      }
    }
    return result;
  }

  private void materializeQueryResult(
      QueryResultMaterializationContext ctx, List<ByteString> outputs, Context context)
      throws IOException {
    if (ctx.fileSinkDesc.getFilesToFetch() == null
        && tryRegisterInternalDagOutput(ctx, outputs, context)) {
      return;
    }
    Path resultDir = ctx.fileSinkDesc.getDirName();
    assert !ctx.fileSinkDesc.isMr3QueryResultLocal() || "file".equalsIgnoreCase(resultDir.toUri().getScheme());

    // if isMr3QueryResultLocal() == true, store the result in the local file system
    // if isMr3QueryResultLocal() == false, we have finished executing a cache-miss query and thus
    // should use the query-results-cache destination directory.
    org.apache.hadoop.fs.FileSystem fs = ctx.fileSinkDesc.isMr3QueryResultLocal()
        ? org.apache.hadoop.fs.FileSystem.getLocal(conf)
        : resultDir.getFileSystem(conf);
    resultDir = fs.makeQualified(resultDir);

    if (fs.exists(resultDir)) {
      fs.delete(resultDir, true);
    }
    fs.mkdirs(resultDir);

    Path resultFile = new Path(resultDir, "000000_0");
    try {
      if (ctx.fileSinkDesc.isUsingBatchingSerDe()) {
        materializeBinaryQueryResult(ctx, resultFile, outputs);
      } else {
        try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(resultFile, true)) {
          for (ByteString output : outputs) {
            output.writeTo(out);
          }
        }
      }
      Set<FileStatus> filesToFetch = ctx.fileSinkDesc.getFilesToFetch();
      if (filesToFetch != null) {
        filesToFetch.add(fs.getFileStatus(resultFile));
      }
    } catch (IOException e) {
      fs.delete(resultDir, true);
      throw e;
    }
  }

  private boolean tryRegisterInternalDagOutput(
      QueryResultMaterializationContext ctx, List<ByteString> outputs, Context context) {
    org.apache.hadoop.hive.ql.plan.TableDesc tableDesc = ctx.fileSinkDesc.getTableInfo();
    if (ctx.fileSinkDesc.isUsingBatchingSerDe()
        || tableDesc.getInputFileFormatClass() != TextInputFormat.class
        || tableDesc.getOutputFileFormatClass() != HiveIgnoreKeyTextOutputFormat.class
        || tableDesc.getSerDeClass() != LazySimpleSerDe.class) {
      LOG.info("Materializing internal MR3 DAG output with its existing result contract for resultId={}, "
              + "inputFormat={}, outputFormat={}, serde={}, batching={}",
          ctx.resultId, tableDesc.getInputFileFormatClass().getName(),
          tableDesc.getOutputFileFormatClass().getName(), tableDesc.getSerdeClassName(),
          ctx.fileSinkDesc.isUsingBatchingSerDe());
      return false;
    }
    context.registerInternalDagOutput(ctx.fileSinkDesc.getDirName(),
        new Context.InternalDagOutput(outputs, getRowSeparator(tableDesc.getProperties())));
    return true;
  }

  private static int getRowSeparator(java.util.Properties tableProperties) {
    String rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n");
    try {
      return Byte.parseByte(rowSeparatorString) & 0xff;
    } catch (NumberFormatException e) {
      return rowSeparatorString.charAt(0) & 0xff;
    }
  }

  @SuppressWarnings("unchecked")
  private void materializeBinaryQueryResult(
      QueryResultMaterializationContext ctx, Path resultFile, List<ByteString> outputs) throws IOException {
    JobConf jc = new JobConf(conf);
    org.apache.hadoop.hive.ql.plan.TableDesc tableDesc = ctx.fileSinkDesc.getTableInfo();
    org.apache.hadoop.hive.ql.io.HiveOutputFormat outputFormat =
        org.apache.hadoop.util.ReflectionUtils.newInstance(
            tableDesc.getOutputFileFormatClass().asSubclass(
                org.apache.hadoop.hive.ql.io.HiveOutputFormat.class), jc);
    FileSinkOperator.RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
        jc, resultFile, org.apache.hadoop.io.BytesWritable.class, false,
        tableDesc.getProperties(), null);
    try {
      for (ByteString output : outputs) {
        try (DataInputStream in = new DataInputStream(output.newInput())) {
          while (in.available() > 0) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            recordWriter.write(new org.apache.hadoop.io.BytesWritable(bytes));
          }
        }
      }
    } finally {
      recordWriter.close(false);
    }
  }

  private static final class QueryResultMaterializationContext {
    final String resultId;
    final FileSinkDesc fileSinkDesc;

    QueryResultMaterializationContext(String resultId, FileSinkDesc fileSinkDesc) {
      this.resultId = resultId;
      this.fileSinkDesc = fileSinkDesc;
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
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    return returnCode;
  }

  private Map<String, JSONObject> buildOperatorGraphMap(TezWork tezWork) {
    Map<String, JSONObject> result = new HashMap<>();

    for (BaseWork work : tezWork.getAllWorkUnsorted()) {
      JSONObject vertexOperatorGraph = new JSONObject();   // VertexOperatorGraph

      JSONObject operatorMap = new JSONObject();
      Set<Operator<?>> ops = OperatorUtils.getOp(work, Operator.class);
      for (Operator<?> op : ops) {
        JSONObject opJson = new JSONObject()
          .put("operatorType", op.getName())
          .put("className", op.getClass().getSimpleName());
        if (op.getConf() instanceof ReduceSinkDesc) {
          opJson.put("outputVertex", ((ReduceSinkDesc)op.getConf()).getOutputName());
        }

        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_INCLUDE_OPERATOR_EXTRA)) {
          Map<String, String> extraInfo = extractOperatorExtraInfo(op);
          if (!extraInfo.isEmpty()) {
            opJson.put("extraInfo", new JSONObject(extraInfo));
          }
        }

        operatorMap.put(op.getOperatorId(), opJson);
      }
      vertexOperatorGraph.put("operatorMap", operatorMap);

      JSONArray operatorEdges = new JSONArray();
      for (Operator<?> op : ops) {
        if (op.getChildOperators() != null) {
          for (Operator<?> child : op.getChildOperators()) {
            JSONObject edge = new JSONObject()
              .put("fromOperatorId", op.getOperatorId())
              .put("toOperatorId", child.getOperatorId());
            operatorEdges.put(edge);
          }
        }
      }
      vertexOperatorGraph.put("operatorEdges", operatorEdges);

      // OperatorGraph: ["vertexMap" --> [vertex name -> VertexOperatorGraph]]
      JSONObject vertexMap = new JSONObject();
      vertexMap.put(work.getName(), vertexOperatorGraph);
      JSONObject operatorGraph = new JSONObject().put("vertexMap", vertexMap);

      result.put(work.getName(), operatorGraph);
    }

    return result;
  }

  private <K> String mapToString(Map<K, String> map) {
    if (map == null || map.isEmpty()) {
      return "";
    }
    return map.entrySet().stream()
      .map(e -> e.getKey() + ": " + e.getValue())
      .collect(Collectors.joining("; "));
  }

  private <T> String listToString(List<T> list, java.util.function.Function<T, String> transformer) {
    if (list == null || list.isEmpty()) {
      return "";
    }
    return list.stream()
      .map(transformer)
      .collect(Collectors.joining(", "));
  }

  private Map<String, String> extractOperatorExtraInfo(Operator<?> op) {
    Map<String, String> extraInfo = new HashMap<>();
    String opName = op.getName();

    try {
      switch (opName) {
        case "TS":
          TableScanDesc tsDesc = (TableScanDesc) op.getConf();
          extraInfo.put("qualifiedTable", String.valueOf(tsDesc.getQualifiedTable()));
          if (tsDesc.getFilterExprString() != null) {
            extraInfo.put("filterExpr", tsDesc.getFilterExprString());
          }
          break;

        case "GBY":
          GroupByDesc gbyDesc = (GroupByDesc) op.getConf();
          extraInfo.put("mode", gbyDesc.getModeString());
          extraInfo.put("keys", gbyDesc.getKeyString());
          extraInfo.put("aggregators", String.join(", ", gbyDesc.getAggregatorStrings()));
          extraInfo.put("outputColumns", String.join(", ", (gbyDesc.getOutputColumnNames())));
          if (gbyDesc.getListGroupingSets() != null) {
            extraInfo.put("groupingSets", listToString(gbyDesc.getListGroupingSets(), String::valueOf));
          }
          extraInfo.put("pruneGroupingSetId", String.valueOf(gbyDesc.pruneGroupingSetId()));
          extraInfo.put("bucketGroup", String.valueOf(gbyDesc.getBucketGroup()));
          break;

        case "FIL":
          FilterDesc filDesc = (FilterDesc) op.getConf();
          extraInfo.put("predicate", filDesc.getPredicateString());
          extraInfo.put("isSampling", String.valueOf(filDesc.getIsSamplingPred()));
          extraInfo.put("isGenerated", String.valueOf(filDesc.isGenerated()));
          break;

        case "SEL":
          SelectDesc selDesc = (SelectDesc) op.getConf();
          extraInfo.put("columnList", selDesc.getColListString());
          extraInfo.put("outputColumns", String.join(", ", selDesc.getOutputColumnNames()));
          extraInfo.put("isSelectStar", String.valueOf(selDesc.isSelectStar()));
          extraInfo.put("isSelStarNoCompute", String.valueOf(selDesc.isSelStarNoCompute()));
          break;

        case "MAPJOIN":
        case "MERGEJOIN":
          MapJoinDesc mjDesc = (MapJoinDesc) op.getConf();
          extraInfo.put("conditions", listToString(mjDesc.getCondsList(), JoinCondDesc::getJoinCondString));
          extraInfo.put("keys", mapToString(mjDesc.getKeysString()));
          for (Map.Entry<Byte, List<ExprNodeDesc>> entry : mjDesc.getExprs().entrySet()) {
            extraInfo.put("value[" + entry.getKey() + "]", PlanUtils.getExprListString(entry.getValue()));
          }
          extraInfo.put("parentToInput", mapToString(mjDesc.getParentToInput()));
          extraInfo.put("keyCounts", mjDesc.getKeyCountsExplainDesc());
          extraInfo.put("posBigTable", String.valueOf(mjDesc.getPosBigTable()));
          extraInfo.put("isBucketMapJoin", String.valueOf(mjDesc.isBucketMapJoin()));
          extraInfo.put("isDynamicPartitionHashJoin", String.valueOf(mjDesc.isDynamicPartitionHashJoin()));
          break;

        case "RS":
          ReduceSinkDesc rsDesc = (ReduceSinkDesc) op.getConf();
          extraInfo.put("outputName", rsDesc.getOutputName());
          extraInfo.put("keyColumns", rsDesc.getKeyColString());
          extraInfo.put("valueColumns", rsDesc.getValueColsString());
          extraInfo.put("partitionColumns", rsDesc.getParitionColsString());
          extraInfo.put("order", rsDesc.getOrder());
          extraInfo.put("numReducers", String.valueOf(rsDesc.getNumReducers()));
          extraInfo.put("tag", String.valueOf(rsDesc.getTag()));
          extraInfo.put("topN", String.valueOf(rsDesc.getTopN()));
          extraInfo.put("isAutoParallel", String.valueOf(rsDesc.isAutoParallel()));
          extraInfo.put("isPTFReduceSink", String.valueOf(rsDesc.isPTFReduceSink()));
          break;

        case "EVENT":
          AppMasterEventDesc eventDesc = (AppMasterEventDesc) op.getConf();
          extraInfo.put("table", eventDesc.getTable().toString());
          extraInfo.put("vertexName", String.valueOf(eventDesc.getVertexName()));
          extraInfo.put("inputName", String.valueOf(eventDesc.getInputName()));
          break;

        case "PTF":
          PTFDesc ptfDesc = (PTFDesc) op.getConf();
          if (ptfDesc.getFuncDef() != null) {
            extraInfo.put("partition", ptfDesc.getFuncDef().getPartitionExplain());
            extraInfo.put("order", ptfDesc.getFuncDef().getOrderExplain());
            extraInfo.put("args", ptfDesc.getFuncDef().getArgsExplain());
          }
          extraInfo.put("llInfo", ptfDesc.getLlInfoExplain());
          break;

        case "TNK":
          TopNKeyDesc tnkDesc = (TopNKeyDesc) op.getConf();
          extraInfo.put("topN", String.valueOf(tnkDesc.getTopN()));
          extraInfo.put("keys", tnkDesc.getKeyString());
          extraInfo.put("columnSortOrder", tnkDesc.getColumnSortOrder());
          extraInfo.put("nullOrder", tnkDesc.getNullOrder());
          break;

        case "JOIN":
          JoinDesc joinDesc = (JoinDesc) op.getConf();
          extraInfo.put("keys", mapToString(joinDesc.getKeysString()));
          extraInfo.put("filters", String.valueOf(joinDesc.getFiltersStringMap()));
          extraInfo.put("outputColumns", String.join(", ", joinDesc.getOutputColumnNames()));
          extraInfo.put("conditions", listToString(joinDesc.getCondsList(), JoinCondDesc::getJoinCondString));
          break;

        case "LIM":
          LimitDesc limitDesc = (LimitDesc) op.getConf();
          extraInfo.put("limit", String.valueOf(limitDesc.getLimit()));
          break;

        case "FS":
          FileSinkDesc fileSinkDesc = (FileSinkDesc) op.getConf();
          extraInfo.put("dirName", fileSinkDesc.getDirNameString());
          extraInfo.put("compressed", String.valueOf(fileSinkDesc.getCompressed()));
          break;
      }

      // Add column expression map for all operators
      if (op.getConf() instanceof AbstractOperatorDesc) {
        AbstractOperatorDesc desc = (AbstractOperatorDesc) op.getConf();
        if (desc.getColumnExprMap() != null && !desc.getColumnExprMap().isEmpty()) {
          extraInfo.put("columnExprMap", mapToString(desc.getColumnExprMapForExplain()));
        }
      }
    } catch (Exception e) {
      // Log error but don't fail the entire operation
      extraInfo.put("extractionError", e.getMessage());
    }

    return extraInfo;
  }
}
