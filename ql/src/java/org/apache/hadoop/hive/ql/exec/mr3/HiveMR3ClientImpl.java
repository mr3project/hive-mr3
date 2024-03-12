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

import com.datamonad.mr3.api.common.MR3Conf$;
import com.datamonad.mr3.api.common.MR3ConfBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRefImpl;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.client.DAGClient;
import com.datamonad.mr3.api.client.MR3SessionClient;
import com.datamonad.mr3.api.client.MR3SessionClient$;
import com.datamonad.mr3.api.client.SessionStatus$;
import com.datamonad.mr3.api.common.MR3Conf;
import com.datamonad.mr3.api.common.MR3Exception;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveMR3ClientImpl implements HiveMR3Client {
  protected static final Logger LOG = LoggerFactory.getLogger(HiveMR3ClientImpl.class);

  // HiveMR3Client can be shared by several threads (from MR3Tasks), and can be closed by any of these
  // threads at any time. After mr3Client.close() is called, all subsequent calls to mr3Client end up
  // with IllegalArgumentException from require{} checking.

  private final MR3SessionClient mr3Client;
  private final HiveConf hiveConf;

  // initAmLocalResources[]: read-only
  HiveMR3ClientImpl(
      String sessionId,
      final Credentials amCredentials,
      final Map<String, LocalResource> amLocalResources,
      final Credentials additionalSessionCredentials,
      final Map<String, LocalResource> additionalSessionLocalResources,
      HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    String prefix = HiveConf.getVar(hiveConf, HiveConf.ConfVars.MR3_APPLICATION_NAME_PREFIX);
    String appName = (prefix != null && !prefix.isEmpty()) ? prefix + "-" + sessionId : sessionId;
    MR3Conf mr3Conf = createMr3Conf(hiveConf);
    scala.collection.immutable.Map amLrs = MR3Utils.toScalaMap(amLocalResources);
    scala.collection.immutable.Map addtlSessionLrs = MR3Utils.toScalaMap(additionalSessionLocalResources);
    mr3Client = MR3SessionClient$.MODULE$.apply(
        appName, mr3Conf,
        Option.apply(amCredentials), amLrs,
        Option.apply(additionalSessionCredentials), addtlSessionLrs);
  }

  public ApplicationId start() throws MR3Exception {
    mr3Client.start();
    return mr3Client.getApplicationId();
  }

  public void connect(ApplicationId appId) throws MR3Exception {
    mr3Client.connect(appId);
  }

  private MR3Conf createMr3Conf(HiveConf hiveConf) {
    JobConf jobConf = new JobConf(new TezConfiguration(hiveConf));
    // TODO: why not use the following?
    // DAGUtils dagUtils = DAGUtils.getInstance();
    // JobConf jobConf = dagUtils.createConfiguration(hiveConf);

    float maxJavaHeapFraction = HiveConf.getFloatVar(hiveConf,
        HiveConf.ConfVars.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION);

    // precedence: (hive-site.xml + command-line options) -> tez-site.xml/mapred-site.xml -> mr3-site.xml
    return new MR3ConfBuilder(true)
        .addResource(jobConf)
        .set(MR3Conf$.MODULE$.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION(), Float.toString(maxJavaHeapFraction))
        .setBoolean(MR3Conf$.MODULE$.MR3_AM_SESSION_MODE(), true).build();
  }

  // Exception if mr3Client is already closed
  @Override
  public MR3JobRef submitDag(
      final DAGAPI.DAGProto dagProto,
      final Credentials amCredentials,
      final Map<String, LocalResource> amLocalResources,
      final Map<String, BaseWork> workMap,
      final DAG dag,
      final Context ctx,
      AtomicBoolean isShutdown) throws Exception {

    scala.collection.immutable.Map addtlAmLrs = MR3Utils.toScalaMap(amLocalResources);
    DAGClient dagClient = mr3Client.submitDag(addtlAmLrs, Option.apply(amCredentials), dagProto);
    return new MR3JobRefImpl(hiveConf, dagClient, workMap, dag, ctx, isShutdown);
  }

  // terminateApplication is irrelevant to whether start() has been called or connect() has been called.
  // ex. start() --> terminateApplication == false if the current instance is no longer a leader.
  // ex. connect() --> terminateApplication = true if the current instance has become a new leader.
  @Override
  public void close(boolean terminateApplication) {
    try {
      if (terminateApplication) {
        LOG.info("HiveMR3Client.close() terminates the current Application");
        mr3Client.shutdownAppMasterToTerminateApplication();
      }
      LOG.info("HiveMR3Client.close() closes MR3SessionClient");
      mr3Client.close();
    } catch (Exception e) {
      // Exception if mr3Client is already closed
      LOG.warn("Failed to close MR3Client", e);
    }
  }

  // Exception if mr3Client is already closed
  @Override
  public MR3ClientState getClientState() throws Exception {
    SessionStatus$.Value sessionState = mr3Client.getSessionStatus();

    LOG.info("MR3ClientState: " + sessionState);

    if (sessionState == SessionStatus$.MODULE$.Initializing()) {
      return MR3ClientState.INITIALIZING;
    } else if (sessionState == SessionStatus$.MODULE$.Ready()
        || sessionState == SessionStatus$.MODULE$.Running()) {
      return MR3ClientState.READY;
    } else {
      return MR3ClientState.SHUTDOWN;
    }
  }

  // Exception if mr3Client is already closed
  @Override
  public boolean isRunningFromApplicationReport() throws Exception {
    ApplicationReport applicationReport = mr3Client.getApplicationReport().getOrElse(null); // == .orNull
    if (applicationReport == null) {
      return false;
    } else {
      YarnApplicationState state = applicationReport.getYarnApplicationState();
      LOG.info("YarnApplicationState from ApplicationReport: " + state);
      switch (state) {
        case FINISHED:
        case FAILED:
        case KILLED:
          return false;
        default:
          return true;
      }
    }
  }

  @Override
  public int getEstimateNumTasksOrNodes(int taskMemoryInMb) throws Exception {
    // getNumContainerWorkers() returns an estimate number of Tasks if taskMemoryInMb > 0
    // getNumContainerWorkers() returns the number of Nodes if taskMemoryInMb <= 0
    return mr3Client.getNumContainerWorkers(taskMemoryInMb);
  }
}
