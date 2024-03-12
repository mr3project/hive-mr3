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

import com.datamonad.mr3.api.common.MR3Exception;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface HiveMR3Client {

  enum MR3ClientState {
    INITIALIZING, READY, SHUTDOWN
  }

  ApplicationId start() throws MR3Exception;

  void connect(ApplicationId appId) throws MR3Exception;

  /**
   * @param dagProto
   * @param amLocalResources
   * @return MR3JobRef could be used to track MR3 job progress and metrics.
   * @throws Exception
   */
  MR3JobRef submitDag(
      DAGAPI.DAGProto dagProto,
      Credentials amCredentials,
      Map<String, LocalResource> amLocalResources,
      Map<String, BaseWork> workMap,
      DAG dag,
      Context ctx,
      AtomicBoolean isShutdown) throws Exception;

  /**
   * @return MR3 client state
   */
  MR3ClientState getClientState() throws Exception;

  // terminateApplication == true: terminate the current Application by shutting down DAGAppMaster
  void close(boolean terminateApplication);

  boolean isRunningFromApplicationReport() throws Exception;

  int getEstimateNumTasksOrNodes(int taskMemoryInMb) throws Exception;
}
