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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DataSource;
import org.apache.hadoop.hive.ql.exec.mr3.dag.EdgeProperty;
import org.apache.hadoop.hive.ql.exec.mr3.dag.EntityDescriptor;
import org.apache.hadoop.hive.ql.exec.mr3.dag.TaskLocationHint;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class MR3Utils {

  public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
    return scala.collection.JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
      scala.Predef.<scala.Tuple2<A, B>>conforms());
  }

  public static ByteString createUserPayloadFromVertexConf(
      CustomVertexConfiguration vertexConf) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    vertexConf.write(dob);
    byte[] userPayload = dob.getData();
    return ByteString.copyFrom(userPayload);
  }

  private static ByteString createUserPayloadFromByteBuffer(ByteBuffer bytes) {
    if (bytes != null) {
      return ByteString.copyFrom(bytes);
    } else {
      return null;
    }
  }

  public static EntityDescriptor convertTezEntityDescriptor(
      org.apache.tez.dag.api.EntityDescriptor ed) {
    if (ed != null) {
      return new EntityDescriptor(
          ed.getClassName(),
          MR3Utils.createUserPayloadFromByteBuffer(
              ed.getUserPayload() != null? ed.getUserPayload().getPayload() : null));
    } else {
      return null;
    }
  }

  public static DataSource convertTezDataSourceDescriptor(
      org.apache.tez.dag.api.DataSourceDescriptor src) {
    EntityDescriptor logicalInputDescriptor =
        MR3Utils.convertTezEntityDescriptor(src.getInputDescriptor());
    EntityDescriptor inputInitializerDescriptor =
        MR3Utils.convertTezEntityDescriptor(src.getInputInitializerDescriptor());

    Credentials credentials = src.getCredentials();

    int numShards = src.getNumberOfShards();

    List<TaskLocationHint> taskLocationHints = null;
    if (src.getLocationHint() != null &&
        src.getLocationHint().getTaskLocationHints() != null) {
      taskLocationHints = Lists.transform(src.getLocationHint().getTaskLocationHints(),
          new Function<org.apache.tez.dag.api.TaskLocationHint, TaskLocationHint>() {
            @Override
            public TaskLocationHint apply(org.apache.tez.dag.api.TaskLocationHint hint) {
              return new TaskLocationHint(hint.getHosts(), hint.getRacks());
            }
          });
    }

    return new DataSource(
        logicalInputDescriptor,
        inputInitializerDescriptor,
        credentials,
        numShards,
        taskLocationHints);
  }

  public static EdgeProperty convertTezEdgeProperty(org.apache.tez.dag.api.EdgeProperty ep) {
    EdgeProperty.DataMovementType dataMovementType;
    switch (ep.getDataMovementType()) {
      case ONE_TO_ONE:
        dataMovementType = EdgeProperty.DataMovementType.ONE_TO_ONE;
        break;
      case BROADCAST:
        dataMovementType = EdgeProperty.DataMovementType.BROADCAST;
        break;
      case SCATTER_GATHER:
        dataMovementType = EdgeProperty.DataMovementType.SCATTER_GATHER;
        break;
      default:
        dataMovementType = EdgeProperty.DataMovementType.CUSTOM;
        break;
    }

    EntityDescriptor srcLogicalOutputDescriptor =
        MR3Utils.convertTezEntityDescriptor(ep.getEdgeSource());
    EntityDescriptor destLogicalInputDescriptor =
        MR3Utils.convertTezEntityDescriptor(ep.getEdgeDestination());
    EntityDescriptor edgeManagerPluginDescriptor =
        MR3Utils.convertTezEntityDescriptor(ep.getEdgeManagerDescriptor());

    return new EdgeProperty(
        dataMovementType,
        srcLogicalOutputDescriptor,
        destLogicalInputDescriptor,
        edgeManagerPluginDescriptor);
  }
}
