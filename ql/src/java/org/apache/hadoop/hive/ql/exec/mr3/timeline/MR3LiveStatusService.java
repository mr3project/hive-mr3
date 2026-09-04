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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC-backed live-status service
 */
public class MR3LiveStatusService implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MR3LiveStatusService.class);

  private final MR3SessionClient mr3SessionClient;

  public MR3LiveStatusService() {
    MR3Session mr3Session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
    mr3SessionClient = mr3Session == null ? null : mr3Session.getMR3SessionClient();
    if (mr3SessionClient == null) {
      LOG.warn("MR3SessionClient unavailable");
    }
  }

  public String getApplicationAttemptId() {
    if (mr3SessionClient == null) {
      return null;
    }
    return mr3SessionClient.getAppAttemptIdStr();
  }

  public AppAttemptStatus getAppAttemptStatus() {
    if (mr3SessionClient == null) {
      return null;
    }
    return mr3SessionClient.getAppAttemptStatus();
  }

  // return null if DAGStatus does not exist or the operation is not permitted
  public DAGStatus getDagStatus(
      int dagIdId, boolean includeCounters, String remoteUser) {
    if (mr3SessionClient == null) {
      return null;
    }
    return mr3SessionClient.getDagStatusWithDagIdId(dagIdId, includeCounters, remoteUser);
  }

  // return null if VertexStatus does not exist or the operation is not permitted
  public VertexStatus getVertexStatus(
      int dagIdId, String vertexName, boolean includeCounters, String remoteUser) {
    if (mr3SessionClient == null) {
      return null;
    }
    return mr3SessionClient.getVertexStatusWithDagIdId(dagIdId, vertexName, includeCounters, remoteUser);
  }

  @Override
  public void close() {
  }
}
