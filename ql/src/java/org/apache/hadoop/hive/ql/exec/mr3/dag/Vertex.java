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
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import com.datamonad.mr3.DAGAPI;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Vertex {

  static public enum VertexExecutionContext {
    EXECUTE_IN_CONTAINER,
    EXECUTE_IN_LLAP,
    EXECUTE_IN_AM
  }

  private final String name;
  private final EntityDescriptor processorDescriptor;
  private final int numTasks;
  private final Resource taskResource;
  private final String containerEnvironment;
  private final String containerJavaOpts;
  private final boolean isMapVertex;
  private final VertexExecutionContext executionContext;

  private final Collection<LocalResource> localResources = new HashSet<LocalResource>();
  private final List<Vertex> inputVertices = new ArrayList<Vertex>();
  private final List<Vertex> outputVertices = new ArrayList<Vertex>();
  private final List<Edge> inputEdges = new ArrayList<Edge>();
  private final List<Edge> outputEdges = new ArrayList<Edge>();
  private final Map<String, DataSource> dataSources = new HashMap<String, DataSource>();
  private final Map<String, Pair<EntityDescriptor, EntityDescriptor>> dataSinks =
          new HashMap<String, Pair<EntityDescriptor, EntityDescriptor>>();

  private EntityDescriptor vertexManagerPluginDescriptor = null;

  private int distanceFromRoot = -1;  // not calculated yet
  private int hasReducerFromRoot = -1;  // -1 == unknown, 0 == false, 1 == true

  private Vertex(
      String name,
      EntityDescriptor processorDescriptor,
      int numTasks,
      Resource taskResource,
      @Nullable String containerEnvironment,
      @Nullable String containerJavaOpts,
      boolean isMapVertex,
      VertexExecutionContext executionContext) {
    this.name = name;
    this.processorDescriptor= processorDescriptor;
    this.numTasks = numTasks;
    this.taskResource = taskResource;
    this.containerEnvironment = containerEnvironment;
    this.containerJavaOpts = containerJavaOpts;
    this.isMapVertex = isMapVertex;
    this.executionContext = executionContext;
  }

  public static Vertex create(
      String name,
      EntityDescriptor processorDescriptor,
      int numTasks,
      Resource taskResource,
      @Nullable String containerEnvironment,
      @Nullable String containerJavaOpts,
      boolean isMapVertex,
      Vertex.VertexExecutionContext executionContext) {
    return new Vertex(
        name, processorDescriptor, numTasks, taskResource,
        containerEnvironment, containerJavaOpts, isMapVertex, executionContext);
  }

  public String getName() {
    return this.name;
  }

  public boolean isMapVertex() {
    return this.isMapVertex;
  }

  public Resource getTaskResource() {
    return taskResource;
  }

  @Nullable
  public String getContainerJavaOpts() {
    return containerJavaOpts;
  }

  @Nullable
  public String getContainerEnvironment() {
    return containerEnvironment;
  }

  public void setVertexManagerPlugin(EntityDescriptor vertexManagerPluginDescriptor) {
    this.vertexManagerPluginDescriptor = vertexManagerPluginDescriptor;
  }

  public EntityDescriptor getVertexManagerPlugin() {
    return vertexManagerPluginDescriptor;
  }

  public void addLocalResources(Collection<LocalResource> vertexLocalResources){
    localResources.addAll(vertexLocalResources);
  }

  public void addDataSource(String name, DataSource dataSource) {
    dataSources.put(name, dataSource);
  }

  /**
   * @return unmodifiableMap of DataSources
   */
  public Map<String, DataSource> getDataSources() {
    return Collections.unmodifiableMap(dataSources);
  }

  public void addDataSink(String name, EntityDescriptor logicalOutputDescriptor) {
    addDataSink(name, logicalOutputDescriptor, null);
  }

  public void addDataSink(String name, EntityDescriptor logicalOutputDescriptor, EntityDescriptor outputCommitterDescriptor) {
    dataSinks.put(name, Pair.of(logicalOutputDescriptor, outputCommitterDescriptor));
  }

  public void addInputEdge(Edge edge) {
    inputVertices.add(edge.getSrcVertex());
    inputEdges.add(edge);
  }

  /**
   * @return unmodifiableList of InputEdges
   */
  public List<Edge> getInputEdges() {
    return Collections.unmodifiableList(inputEdges);
  }

  public void addOutputEdge(Edge edge) {
    outputVertices.add(edge.getDestVertex());
    outputEdges.add(edge);
  }

  /**
   * Get the input vertices for this vertex
   * @return List of input vertices
   */
  public List<Vertex> getInputVertices() {
    return Collections.unmodifiableList(inputVertices);
  }

  /**
   * Get the output vertices for this vertex
   * @return List of output vertices
   */
  public List<Vertex> getOutputVertices() {
    return Collections.unmodifiableList(outputVertices);
  }

  int getDistanceFromRoot() {
    if (distanceFromRoot >= 0) {
      return distanceFromRoot;
    } else {
      int maxDistanceFromRoot = 0;
      for (Edge edge: getInputEdges()) {
        int distanceFromRoot = 1 + edge.getSrcVertex().getDistanceFromRoot();
        maxDistanceFromRoot = Math.max(maxDistanceFromRoot, distanceFromRoot);
      }
      distanceFromRoot = maxDistanceFromRoot;
      return maxDistanceFromRoot;
    }
  }

  int getHasReducerFromRoot() {
    if (hasReducerFromRoot >= 0) {
      return hasReducerFromRoot;
    } else {
      if (!isMapVertex) {
        hasReducerFromRoot = 1;
        return hasReducerFromRoot;
      } else {
        for (Edge edge: getInputEdges()) {
          if (edge.getSrcVertex().getHasReducerFromRoot() == 1) {
            hasReducerFromRoot = 1;
            return hasReducerFromRoot;
          }
        }
        hasReducerFromRoot = 0;
        return hasReducerFromRoot;
      }
    }
  }

  String getContainerGroupName(DAG.ContainerGroupScheme scheme) {
    if (scheme == DAG.ContainerGroupScheme.ALL_IN_ONE) {
      return DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME;
    } else if (scheme == DAG.ContainerGroupScheme.PER_MAP_REDUCE) {
      if (getHasReducerFromRoot() == 0) {
        return DAG.PER_MAP_CONTAINER_GROUP_NAME;
      } else {
        return DAG.PER_REDUCE_CONTAINER_GROUP_NAME;
      }
    } else {
      return name;
    }
  }

  DAGAPI.VertexProto createVertexProto(String containerGroupName, int vcoresDivisor) {
    Function<Edge, String> transformEdgeToIdFunc = new Function<Edge, String>() {
      @Override
      public String apply(Edge edge) { return edge.getId(); }
    };

    DAGAPI.VertexProto vertexProto = DAGAPI.VertexProto.newBuilder()
        .setName(name)
        .setProcessor(processorDescriptor.createEntityDescriptorProto())
        .setVertexManagerPlugin(vertexManagerPluginDescriptor.createEntityDescriptorProto())
        .setContainerGroupName(containerGroupName)
        .setNumTasks(numTasks)
        .setResource(createResourceProto(vcoresDivisor))
        // do not set UniquePerNode
        .setPriority(getDistanceFromRoot() * 3)
        .addAllInEdgeIds(Lists.transform(inputEdges, transformEdgeToIdFunc))
        .addAllOutEdgeIds(Lists.transform(outputEdges, transformEdgeToIdFunc))
        .addAllRootInputs(createRootInputProtos())
        .addAllLeafOutputs(createLeafOutputProtos())
        .addAllTaskLocationHints(createTaskLocationHintProtos())
        .build();

    return vertexProto;
  }

  private DAGAPI.ResourceProto createResourceProto(int vcoresDivisor) {
    return DAGAPI.ResourceProto.newBuilder()
        .setMemoryMb(getTaskResource().getMemory())
        .setVirtualCores(getTaskResource().getVirtualCores())
        .setCoreDivisor(vcoresDivisor)
        .build();
  }

  Credentials getAggregatedCredentials() {
    Credentials aggregatedCredentials = new Credentials();

    for (DataSource dataSource: dataSources.values()) {
      if (dataSource.getCredentials() != null) {
        aggregatedCredentials.addAll(dataSource.getCredentials());
      }
    }

    return aggregatedCredentials;
  }

  private List<DAGAPI.TaskLocationHintProto> createTaskLocationHintProtos() {
    List<DAGAPI.TaskLocationHintProto> taskLocationHintProtos =
        new ArrayList<DAGAPI.TaskLocationHintProto>();

    // TODO: MR3 Tez Dag.creteDagProto() get TaskLocationHits only from DataSource[0]
    // It seems that (in hive-mr3) a vertex will have only one dataSource, but it is possible in
    // future for supporting some join optimizations.
    for ( DataSource dataSource: dataSources.values() ) {
      taskLocationHintProtos.addAll(dataSource.createTaskLocationHintProtos());
    }

    return taskLocationHintProtos;
  }

  private List<DAGAPI.RootInputProto> createRootInputProtos() {
    List<DAGAPI.RootInputProto> rootInputProto = new ArrayList<DAGAPI.RootInputProto>();

    for (Map.Entry<String, DataSource> dsEntry: dataSources.entrySet()) {
      rootInputProto.add(dsEntry.getValue().createRootInputProto(dsEntry.getKey()));
    }

    return rootInputProto;
  }

  private List<DAGAPI.LeafOutputProto> createLeafOutputProtos() {
    List<DAGAPI.LeafOutputProto> leafOutputProtos = new ArrayList<DAGAPI.LeafOutputProto>();

    for ( Map.Entry<String, Pair<EntityDescriptor, EntityDescriptor>> entry: dataSinks.entrySet() ) {
      DAGAPI.LeafOutputProto.Builder builder = DAGAPI.LeafOutputProto.newBuilder()
          .setName(entry.getKey())
          .setLogicalOutput(entry.getValue().getLeft().createEntityDescriptorProto());
      if (entry.getValue().getRight() != null) {
        builder.setOutputCommitter(entry.getValue().getRight().createEntityDescriptorProto());
      }
      DAGAPI.LeafOutputProto leafOutputProto = builder.build();
      leafOutputProtos.add(leafOutputProto);
    }

    return leafOutputProtos;
  }

  public ByteString getProcessorDescriptorPayload() {
    return processorDescriptor.getUserPayload();
  }
}
