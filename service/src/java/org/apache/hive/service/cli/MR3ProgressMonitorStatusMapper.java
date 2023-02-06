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

package org.apache.hive.service.cli;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;

public class MR3ProgressMonitorStatusMapper implements ProgressMonitorStatusMapper {

  /**
   * These states are taken form DAGStatus.State, could not use that here directly as it was
   * optional dependency and did not want to include it just for the enum.
   */
  enum MR3Status {
    New, Inited, Running, Succeeded, Failed, Killed

  }

  @Override
  public TJobExecutionStatus forStatus(String status) {
    if (StringUtils.isEmpty(status)) {
      return TJobExecutionStatus.NOT_AVAILABLE;
    }
    MR3Status mr3Status = MR3Status.valueOf(status);
    switch (mr3Status) {
      case New:
      case Inited:
      case Running:
        return TJobExecutionStatus.IN_PROGRESS;
      default:
        return TJobExecutionStatus.COMPLETE;
    }
  }
}
