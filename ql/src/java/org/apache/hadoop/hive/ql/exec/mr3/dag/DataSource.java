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

import org.apache.hadoop.security.Credentials;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DataSource {

  private final EntityDescriptor logicalInputDescriptor;
  private final EntityDescriptor inputInitializerDescriptor;
  private final Credentials credentials;
  private final int numShards;
  private final List<TaskLocationHint> taskLocationHints;

  public DataSource(
      EntityDescriptor logicalInputDescriptor,
      @Nullable EntityDescriptor inputInitializerDescriptor,
      @Nullable Credentials credentials,
      int numShards,
      @Nullable List<TaskLocationHint> taskLocationHints) {
    this.logicalInputDescriptor = logicalInputDescriptor;
    this.inputInitializerDescriptor = inputInitializerDescriptor;
    this.credentials = credentials;
    this.numShards = numShards;
    this.taskLocationHints = taskLocationHints;
  }

  Credentials getCredentials(){
    return credentials;
  }

  public boolean hasInputInitializer() {
    return inputInitializerDescriptor != null;
  }

  // DAGProto Conversion utilities
  public DAGAPI.RootInputProto createRootInputProto(String name) {
    DAGAPI.RootInputProto.Builder builder = DAGAPI.RootInputProto.newBuilder()
        .setName(name)
        .setLogicalInput(logicalInputDescriptor.createEntityDescriptorProto());

    if (inputInitializerDescriptor != null) {
      builder.setInputInitializer(inputInitializerDescriptor.createEntityDescriptorProto());
    }

    return builder.build();
  }

  public List<DAGAPI.TaskLocationHintProto> createTaskLocationHintProtos() {
    List<DAGAPI.TaskLocationHintProto> taskLocationHintProtos =
        new ArrayList<DAGAPI.TaskLocationHintProto>();

    if (taskLocationHints != null) {
      for (TaskLocationHint taskLocationHint: taskLocationHints) {
        taskLocationHintProtos.add(taskLocationHint.createTaskLocationHintProto());
      }
    }

    return taskLocationHintProtos;
  }
}
