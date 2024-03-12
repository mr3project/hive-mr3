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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VertexGroup {

  private final String name;
  private final Vertex[] members;
  private final List<GroupInputEdge> edges;
  private final List<String> outputs;   // names of LeafOutputs

  public VertexGroup(
      String name,
      Vertex[] members,
      List<GroupInputEdge> edges,
      @Nullable List<String> outputs) {
    this.name = name;
    this.members = members;
    this.edges = edges;
    this.outputs = outputs;
  }

  public String getName() {
    return name;
  }

  public Vertex[] getMembers() {
    return members;
  }

  public List<GroupInputEdge> getEdges() {
    return edges;
  }

  public List<String> getOutputs() {
    return outputs;
  }

  // DAGProto Conversion utilities
  public DAGAPI.VertexGroupProto createVertexGroupProto() {
    DAGAPI.VertexGroupProto.Builder vertexGroupProtoBuilder = DAGAPI.VertexGroupProto.newBuilder()
        .setGroupName(name)
        .addAllGroupMembers(
           Lists.transform(Arrays.asList(members), new Function<Vertex, String>() {
              @Override
              public String apply(Vertex vertex) { return vertex.getName(); }
        }))
        .addAllMergedInputEdges(createMergedInputEdgeProtos());

    if (outputs != null) {
      vertexGroupProtoBuilder.addAllOutputs(outputs);
    }

    return vertexGroupProtoBuilder.build();
  }

  private List<DAGAPI.MergedInputEdgeProto> createMergedInputEdgeProtos() {
    List<DAGAPI.MergedInputEdgeProto> mergedInputEdgeProtos =
        new ArrayList<DAGAPI.MergedInputEdgeProto>();

    for (GroupInputEdge groupInputEdge: edges) {
      mergedInputEdgeProtos.add(groupInputEdge.createMergedInputEdgeProto());
    }

    return mergedInputEdgeProtos;
  }
}
