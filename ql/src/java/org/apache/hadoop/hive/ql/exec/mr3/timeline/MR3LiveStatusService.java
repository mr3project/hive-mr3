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

package org.apache.hadoop.hive.ql.exec.mr3.timeline;

import com.datamonad.mr3.api.client.AppAttemptStatus;
import com.datamonad.mr3.api.client.DAGStatus;
import com.datamonad.mr3.api.client.MR3SessionClient;
import com.datamonad.mr3.api.client.VertexStatus;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * RPC-backed live-status service
 */
public class MR3LiveStatusService implements AutoCloseable {

  private final MR3SessionClient mr3SessionClient;

  public MR3LiveStatusService() {
    MR3Session mr3Session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
    if (mr3Session == null) {
      throw new IllegalStateException("No active MR3Session is available for MR3-UI");
    }
    mr3SessionClient = mr3Session.getMR3SessionClient();
    if (mr3SessionClient == null) {
      throw new IllegalStateException("The active MR3Session has no MR3SessionClient");
    }
    // TODO: Resolve the active MR3Session for each operation so that a long-lived service
    // can follow replacement of the shared session.
  }

  public String getApplicationAttemptId() {
    // TODO: ApplicationAttemptId can be cached after the first RPC
    return "UNKNOWN_APPLICATION_ATTEMPT_ID";
  }

  public AppAttemptStatus getAppAttemptStatus() {
    return null;
  }

  public DAGStatus getDagStatus(
      int dagId, boolean includeCounters, UserGroupInformation callerUGI) {
    return null;
  }

  public VertexStatus getVertexStatus(
      int dagId, String vertexName, boolean includeCounters, UserGroupInformation callerUGI) {
    return null;
  }

  @Override
  public void close() {
  }
}
