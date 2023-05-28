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

public class Edge {

  private final Vertex srcVertex;
  private final Vertex destVertex;
  private final EdgeProperty edgeProperty;

  public Edge(
      Vertex srcVertex,
      Vertex destVertex,
      EdgeProperty edgeProperty) {
    this.srcVertex = srcVertex;
    this.destVertex = destVertex;
    this.edgeProperty = edgeProperty;
  }

  public Vertex getSrcVertex() { return srcVertex; }
  public Vertex getDestVertex() { return destVertex; }
  public EdgeProperty getEdgeProperty() { return edgeProperty; }

  public String getId() {
    return srcVertex.getName() + "-" + destVertex.getName();
  }

  // DAGProto Conversion utilities
  public DAGAPI.EdgeProto createEdgeProto() {
    DAGAPI.EdgeProto.Builder edgeBuilder = DAGAPI.EdgeProto.newBuilder()
        .setId(getId())
        .setInputVertexName(srcVertex.getName())
        .setOutputVertexName(destVertex.getName())
        .setDataMovementType(edgeProperty.createEdgeDataMovementTypeProto())
        .setSrcLogicalOutput(
            edgeProperty.getSrcLogicalOutputDescriptor().createEntityDescriptorProto())
        .setDestLogicalInput(
            edgeProperty.getDestLogicalInputDescriptor().createEntityDescriptorProto());

    if (edgeProperty.getDataMovementType() == EdgeProperty.DataMovementType.CUSTOM) {
      if (edgeProperty.getEdgeManagerPluginDescriptor() != null) {
        edgeBuilder.setEdgeManagerPlugin(
            edgeProperty.getEdgeManagerPluginDescriptor().createEntityDescriptorProto());
      } // else the AM will deal with this.
    }

    return edgeBuilder.build();
  }
}
