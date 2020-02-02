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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MR3Session {

  /**
   * Initializes an MR3 session for DAG execution.
   * May block until Client is initialized and ready for DAG submission
   * @param conf Hive configuration.
   */
  void start(HiveConf conf) throws HiveException;

  void connect(HiveConf conf, ApplicationId appId) throws HiveException;

  ApplicationId getApplicationId();

  /**
   * @param dag
   * @param amLocalResources
   * @param conf
   * @param workMap
   * @return MR3JobRef
   * @throws Exception
   */
  MR3JobRef submit(
      DAG dag,
      Map<String, LocalResource> amLocalResources,
      Configuration conf,
      Map<String, BaseWork> workMap,
      Context ctx,
      AtomicBoolean isShutdown,
      PerfLogger perfLogger) throws Exception;

  /**
   * @return session id.
   */
  String getSessionId();

  /**
   * Close session and release resources.
   */
  void close(boolean terminateApplication);

  /**
   * @return session scratch Directory
   */
  Path getSessionScratchDir();

  boolean isRunningFromApplicationReport();

  int getEstimateNumTasksOrNodes(int taskMemoryInMb) throws Exception;
}
