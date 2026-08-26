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
import com.datamonad.mr3.api.client.VertexStatus;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Live-status service. RPC-backed status retrieval will be added later.
 */
public class MR3LiveStatusService implements MR3LiveStatusServiceInterface {
  private static final MR3LiveStatusService INSTANCE = new MR3LiveStatusService();
  private static final CurrentAttempt INITIAL_ATTEMPT = new CurrentAttempt("");

  public static MR3LiveStatusService getInstance() {
    return INSTANCE;
  }

  @Override
  public CurrentAttempt getCurrentAttempt() {
    return INITIAL_ATTEMPT;
  }

  @Override
  public AppAttemptStatus getAppAttemptStatus() {
    return null;
  }

  @Override
  public DAGStatus getDagStatus(
      int dagId, boolean includeCounters, UserGroupInformation callerUGI) {
    return null;
  }

  @Override
  public VertexStatus getVertexStatus(
      int dagId, String vertexName, boolean includeCounters, UserGroupInformation callerUGI) {
    return null;
  }

  @Override
  public void close() {
  }
}
