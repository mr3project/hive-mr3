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

package org.apache.hadoop.hive.ql.exec.mr3.dag;

import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;

import java.util.Set;

public class TaskLocationHint {

  private final Set<String> hosts;
  private final Set<String> racks;

  public TaskLocationHint(
      Set<String> hosts,
      Set<String> racks) {
    this.hosts = hosts;
    this.racks = racks;
  }

  // DAGProto Conversion utilities
  public DAGAPI.TaskLocationHintProto createTaskLocationHintProto() {
    return DAGAPI.TaskLocationHintProto.newBuilder()
        .addAllHosts(hosts)
        .addAllRacks(racks)
        .build();
  }
}
