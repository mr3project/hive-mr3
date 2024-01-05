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

import com.google.protobuf.ByteString;
import com.datamonad.mr3.api.common.MR3Conf$;
import com.datamonad.mr3.api.common.MR3ConfBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.DAGUtils;
import org.apache.hadoop.hive.ql.exec.mr3.llap.LLAPDaemonProcessor;
import org.apache.hadoop.hive.ql.exec.mr3.llap.LLAPDaemonVertexManagerPlugin;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.common.CommonUtils;
import com.datamonad.mr3.api.common.MR3Conf;
import com.datamonad.mr3.api.util.ProtoConverters;
import com.datamonad.mr3.api.common.Utils$;
import com.datamonad.mr3.tez.shufflehandler.ShuffleHandler;
import com.datamonad.mr3.tez.shufflehandler.ShuffleHandlerDaemonProcessor;
import com.datamonad.mr3.tez.shufflehandler.ShuffleHandlerDaemonVertexManagerPlugin;
import org.apache.tez.dag.api.TezConfiguration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DAG {

  final private String name;
  final private String dagInfo;
  final private Credentials dagCredentials;

  final private Collection<LocalResource> localResources = new HashSet<LocalResource>();
  final private Map<String, Vertex> vertices = new HashMap<String, Vertex>();
  final private List<VertexGroup> vertexGroups = new ArrayList<VertexGroup>();
  final private List<Edge> edges = new ArrayList<Edge>();

  public static enum ContainerGroupScheme { ALL_IN_ONE, PER_MAP_REDUCE, PER_VERTEX }

  public static final String ALL_IN_ONE_CONTAINER_GROUP_NAME = "All-In-One";
  public static final String PER_MAP_CONTAINER_GROUP_NAME = "Per-Map";
  public static final String PER_REDUCE_CONTAINER_GROUP_NAME = "Per-Reduce";

  public static final int allInOneContainerGroupPriority = 0;
  public static final int perMapContainerGroupPriority = 0;
  public static final int perReduceContainerGroupPriority = perMapContainerGroupPriority + 3;

  public static final int defaultLlapDaemonTaskMemoryMb = 0;
  public static final int defaultLlapDaemonTaskVcores = 0;

  private int vcoresDivisor = 1;  // set in createDagProto()

  private DAG(
      String name,
      String dagInfo,
      @Nullable Credentials dagCredentials) {
    this.name = name;
    this.dagInfo = dagInfo;
    this.dagCredentials = dagCredentials != null ? dagCredentials : new Credentials();
  }

  public static DAG create(
      String name,
      String dagInfo,
      Credentials dagCredentials) {
    return new DAG(name, dagInfo, dagCredentials);
  }

  /**
   * adds Paths to Dag Credentials
   * @param paths
   * @throws IOException
   */
  public void addPathsToCredentials(
      DAGUtils dagUtils, Set<Path> paths, Configuration conf) throws IOException {
    dagUtils.addPathsToCredentials(dagCredentials, paths, conf);
  }

  public void addLocalResources(Collection<LocalResource> localResources) {
    this.localResources.addAll(localResources);
  }

  public void addVertex(Vertex vertex) {
    assert !vertices.containsKey(vertex.getName());
    vertices.put(vertex.getName(), vertex);
  }

  /**
   * @return unmodifiableMap of Vertices
   */
  public Map<String, Vertex> getVertices() {
    return Collections.unmodifiableMap(vertices);
  }

  public void addVertexGroup(VertexGroup vertexGroup) {
    vertexGroups.add(vertexGroup);

    for (Vertex member: vertexGroup.getMembers()) {
      for (GroupInputEdge gedge: vertexGroup.getEdges()) {
        Vertex destVertex = gedge.getDestVertex();
        EdgeProperty edgeProperty = gedge.getEdgeProperty();
        Edge edge = new Edge(member, destVertex, edgeProperty);
        addEdge(edge);
      }
    }
  }

  public void addEdge(Edge edge) {
    Vertex srcVertex = edge.getSrcVertex();
    Vertex destVertex = edge.getDestVertex();
    assert vertices.containsKey(srcVertex.getName());
    assert vertices.containsKey(destVertex.getName());

    srcVertex.addOutputEdge(edge);
    destVertex.addInputEdge(edge);
    edges.add(edge);
  }

  public DAGAPI.DAGProto createDagProto(Configuration mr3TaskConf, MR3Conf dagConf) throws IOException {
    this.vcoresDivisor = HiveConf.getIntVar(mr3TaskConf, HiveConf.ConfVars.MR3_RESOURCE_VCORES_DIVISOR);
    ContainerGroupScheme scheme = getContainerGroupScheme(mr3TaskConf);

    List<DAGAPI.VertexProto> vertexProtos = createVertexProtos(scheme);

    List<DAGAPI.EdgeProto> edgeProtos = new ArrayList<DAGAPI.EdgeProto>();
    for (Edge edge: edges) {
      edgeProtos.add(edge.createEdgeProto());
    }

    List<DAGAPI.VertexGroupProto> vertexGroupProtos = new ArrayList<DAGAPI.VertexGroupProto>();
    for (VertexGroup vertexGrp: vertexGroups) {
      vertexGroupProtos.add(vertexGrp.createVertexGroupProto());
    }

    List<DAGAPI.LocalResourceProto> lrProtos = new ArrayList<DAGAPI.LocalResourceProto>();
    DAGUtils dagUtils = DAGUtils.getInstance();
    for (LocalResource lr: localResources) {
      lrProtos.add(ProtoConverters.convertToLocalResourceProto(dagUtils.getBaseName(lr), lr));
    }

    boolean useLlapIo = HiveConf.getBoolVar(mr3TaskConf, HiveConf.ConfVars.LLAP_IO_ENABLED, false);
    int llapMemory = 0;
    int llapCpus = 0;
    DAGAPI.DaemonVertexProto llapDaemonVertexProto = null;
    if (useLlapIo) {
      // llapMemory = 0 and llapCpus = 0 are valid.
      llapMemory = HiveConf.getIntVar(mr3TaskConf, HiveConf.ConfVars.MR3_LLAP_DAEMON_TASK_MEMORY_MB);
      if (llapMemory < 0) {
        llapMemory = defaultLlapDaemonTaskMemoryMb;
      }
      llapCpus = HiveConf.getIntVar(mr3TaskConf, HiveConf.ConfVars.MR3_LLAP_DAEMON_TASK_VCORES);
      if (llapCpus < 0) {
        llapCpus = defaultLlapDaemonTaskVcores;
      }
      // LLAP daemon never needs tez-site.xml, so we do not create JobConf.
      ByteString userPayload = org.apache.tez.common.TezUtils.createByteStringFromConf(mr3TaskConf);
      llapDaemonVertexProto = createLlapDaemonVertexProto(userPayload, llapMemory, llapCpus);
    }

    TezConfiguration tezConf = null;
    List<DAGAPI.DaemonVertexProto> shuffleHandlerDaemonVertexProtos = null;
    if (scheme == DAG.ContainerGroupScheme.ALL_IN_ONE) {
      tezConf = new TezConfiguration(mr3TaskConf);
      int useDaemonShuffleHandler = HiveConf.getIntVar(mr3TaskConf, HiveConf.ConfVars.MR3_USE_DAEMON_SHUFFLEHANDLER);
      if (useDaemonShuffleHandler > 0) {
        ByteString userPayload = org.apache.tez.common.TezUtils.createByteStringFromConf(tezConf);
        shuffleHandlerDaemonVertexProtos = createShuffleHandlerDaemonVertexProto(useDaemonShuffleHandler, userPayload);
      }
    }

    // we do not create containerGroupConf
    // if ALL_IN_ONE, then tezConf != null
    List<DAGAPI.ContainerGroupProto> containerGroupProtos = createContainerGroupProtos(
        mr3TaskConf, scheme, vertices.values(),
        llapMemory, llapCpus, llapDaemonVertexProto,
        shuffleHandlerDaemonVertexProtos, tezConf);

    DAGAPI.ConfigurationProto dagConfProto = Utils$.MODULE$.createMr3ConfProto(dagConf);

    // We should call setDagConf(). Otherwise we would end up using DAGAppMaster.MR3Conf in MR3.
    DAGAPI.DAGProto dagProto = DAGAPI.DAGProto.newBuilder()
        .setName(name)
        .setCredentials(CommonUtils.convertCredentialsToByteString(dagCredentials))
        .setDagInfo(dagInfo)
        .addAllVertices(vertexProtos)
        .addAllEdges(edgeProtos)
        .addAllVertexGroups(vertexGroupProtos)
        .addAllLocalResources(lrProtos)
        .addAllContainerGroups(containerGroupProtos)
        .setDagConf(dagConfProto)
        .build();

    return dagProto;
  }

  private ContainerGroupScheme getContainerGroupScheme(Configuration conf) {
    String scheme = conf.get(HiveConf.ConfVars.MR3_CONTAINERGROUP_SCHEME.varname);
    if (scheme.equals("per-vertex")) {
      return ContainerGroupScheme.PER_VERTEX;
    } else if (scheme.equals("per-map-reduce")) {
      return ContainerGroupScheme.PER_MAP_REDUCE;
    } else {  // defaults to "all-in-one"
      return ContainerGroupScheme.ALL_IN_ONE;
    }
  }

  private List<DAGAPI.VertexProto> createVertexProtos(ContainerGroupScheme scheme) {
    List<DAGAPI.VertexProto> vertexProtos = new ArrayList<DAGAPI.VertexProto>();

    for (Vertex vertex: vertices.values()) {
      // here we add HDFS_DELEGATION_TOKEN to dagCredentials
      dagCredentials.addAll(vertex.getAggregatedCredentials());
      String containerGroupName = vertex.getContainerGroupName(scheme);
      vertexProtos.add(vertex.createVertexProto(containerGroupName, this.vcoresDivisor));
    }

    return vertexProtos;
  }

  private DAGAPI.DaemonVertexProto createLlapDaemonVertexProto(
      ByteString userPayload, int llapMemory, int llapCpus) {
    DAGAPI.ResourceProto resource = DAGAPI.ResourceProto.newBuilder()
        .setMemoryMb(llapMemory)
        .setVirtualCores(llapCpus)
        .setCoreDivisor(this.vcoresDivisor)
        .build();

    String procClassName = LLAPDaemonProcessor.class.getName();
    EntityDescriptor processorDescriptor = new EntityDescriptor(procClassName, userPayload);

    String pluginClassName = LLAPDaemonVertexManagerPlugin.class.getName();
    EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(pluginClassName, null);

    DAGAPI.DaemonVertexProto daemonVertexProto = DAGAPI.DaemonVertexProto.newBuilder()
        .setName("LLAP")
        .setResource(resource)
        .setProcessor(processorDescriptor.createEntityDescriptorProto())
        .setVertexManagerPlugin(vertexManagerPluginDescriptor.createEntityDescriptorProto())
        .build();

    return daemonVertexProto;
  }

  private List<DAGAPI.DaemonVertexProto> createShuffleHandlerDaemonVertexProto(
        int useDaemonShuffleHandler,
        ByteString userPayload) {
    // TODO: introduce MR3_SHUFFLEHANDLER_DAEMON_TASK_MEMORY_MB and MR3_SHUFFLEHANDLER_DAEMON_TASK_VCORES,
    // but only if a performance/stability problem arises from ShuffleHandler
    DAGAPI.ResourceProto resource = DAGAPI.ResourceProto.newBuilder()
        .setMemoryMb(0)
        .setVirtualCores(0)
        .setCoreDivisor(this.vcoresDivisor)
        .build();

    String procClassName = ShuffleHandlerDaemonProcessor.class.getName();
    EntityDescriptor processorDescriptor = new EntityDescriptor(procClassName, userPayload);

    String pluginClassName = ShuffleHandlerDaemonVertexManagerPlugin.class.getName();
    EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(pluginClassName, null);

    List<DAGAPI.DaemonVertexProto> shuffleHandlerDaemonVertexProtos = new ArrayList<DAGAPI.DaemonVertexProto>();
    for (int i = 0; i < useDaemonShuffleHandler; i++) {
      String shuffleVertexName = ShuffleHandler.class.getSimpleName() + "_" + (i + 1);
      DAGAPI.DaemonVertexProto daemonVertexProto = DAGAPI.DaemonVertexProto.newBuilder()
          .setName(shuffleVertexName)
          .setResource(resource)
          .setProcessor(processorDescriptor.createEntityDescriptorProto())
          .setVertexManagerPlugin(vertexManagerPluginDescriptor.createEntityDescriptorProto())
          .build();
      shuffleHandlerDaemonVertexProtos.add(daemonVertexProto);
    }

    return shuffleHandlerDaemonVertexProtos;
  }

  // if ALL_IN_ONE, then tezConf != null
  private List<DAGAPI.ContainerGroupProto> createContainerGroupProtos(
      Configuration mr3TaskConf, ContainerGroupScheme scheme, Collection<Vertex> vertices,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto,
      List<DAGAPI.DaemonVertexProto> shuffleHandlerDaemonVertexProtos, TezConfiguration tezConf) {
    List<DAGAPI.ContainerGroupProto> containerGroupProtos = new ArrayList<DAGAPI.ContainerGroupProto>();

    if (scheme == DAG.ContainerGroupScheme.ALL_IN_ONE) {
      DAGAPI.ContainerGroupProto allInOneContainerGroupProto = createAllInOneContainerGroupProto(
          mr3TaskConf, llapMemory, llapCpus, llapDaemonVertexProto, shuffleHandlerDaemonVertexProtos, tezConf);
      containerGroupProtos.add(allInOneContainerGroupProto);

    } else if (scheme == DAG.ContainerGroupScheme.PER_MAP_REDUCE) {
      DAGAPI.ContainerGroupProto perMapContainerGroupProto =
        createPerMapReduceContainerGroupProto(mr3TaskConf, true, llapMemory, llapCpus, llapDaemonVertexProto);
      DAGAPI.ContainerGroupProto perReduceContainerGroupProto =
        createPerMapReduceContainerGroupProto(mr3TaskConf, false, 0, 0, null);
      containerGroupProtos.add(perMapContainerGroupProto);
      containerGroupProtos.add(perReduceContainerGroupProto);

    } else {
      for(Vertex vertex: vertices) {
        DAGAPI.ContainerGroupProto perVertexContainerGroupProto =
          createPerVertexContainerGroupProto(mr3TaskConf, vertex);
        containerGroupProtos.add(perVertexContainerGroupProto);
      }
    }

    return containerGroupProtos;
  }

  // ALL_IN_ONE, and tezConf != null
  // if shuffleHandlerDaemonVertexProtos != null, useDaemonShuffleHandler == shuffleHandlerDaemonVertexProtos.size()
  private DAGAPI.ContainerGroupProto createAllInOneContainerGroupProto(Configuration conf,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto,
      List<DAGAPI.DaemonVertexProto> shuffleHandlerDaemonVertexProtos, TezConfiguration tezConf) {
    int llapNativeMemoryMb = 0;
    if (llapDaemonVertexProto != null) {
      long ioMemoryBytes = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED) ? 0L :
        HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      int headroomMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_HEADROOM_MB);
      llapNativeMemoryMb = (int)(ioMemoryBytes >> 20) + headroomMb;
    }

    int allLlapMemory = llapMemory + llapNativeMemoryMb;
    DAGAPI.ResourceProto allInOneResource =
      createResourceProto(DAGUtils.getAllInOneContainerGroupResource(conf, allLlapMemory, llapCpus));

    DAGAPI.ContainerConfigurationProto.Builder allInOneContainerConf =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(allInOneResource);
    setJavaOptsEnvironmentStr(conf, allInOneContainerConf);

    if (llapDaemonVertexProto != null) {
      allInOneContainerConf.setNativeMemoryMb(llapNativeMemoryMb);
    }

    int useDaemonShuffleHandler = shuffleHandlerDaemonVertexProtos != null ? shuffleHandlerDaemonVertexProtos.size() : 0;
    DAGAPI.ConfigurationProto containerGroupConfProto =
          getContainerGroupConfProto(conf, useDaemonShuffleHandler, tezConf);
    DAGAPI.ContainerGroupProto.Builder allInOneContainerGroup =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(ALL_IN_ONE_CONTAINER_GROUP_NAME)
          .setContainerConfig(allInOneContainerConf.build())
          .setPriority(allInOneContainerGroupPriority)
          .setContainerGroupConf(containerGroupConfProto);

    if (llapDaemonVertexProto != null || shuffleHandlerDaemonVertexProtos != null) {
      List<DAGAPI.DaemonVertexProto> daemonVertexProtos = new ArrayList<>();
      if (llapDaemonVertexProto != null) {
        daemonVertexProtos.add(llapDaemonVertexProto);
      }
      if (shuffleHandlerDaemonVertexProtos != null) {
        daemonVertexProtos.addAll(shuffleHandlerDaemonVertexProtos);
      }
      allInOneContainerGroup.addAllDaemonVertices(daemonVertexProtos);
    }

    return allInOneContainerGroup.build();
  }

  private DAGAPI.ContainerGroupProto createPerMapReduceContainerGroupProto(
      Configuration conf, boolean isMap,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto) {
    String groupName = isMap ? PER_MAP_CONTAINER_GROUP_NAME : PER_REDUCE_CONTAINER_GROUP_NAME;
    int priority = isMap ? perMapContainerGroupPriority : perReduceContainerGroupPriority;

    int llapNativeMemoryMb = 0;
    if (isMap && llapDaemonVertexProto != null) {
      long ioMemoryBytes = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED) ? 0L :
        HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      int headroomMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_HEADROOM_MB);
      llapNativeMemoryMb = (int)(ioMemoryBytes >> 20) + headroomMb;
    }

    int allLlapMemory = llapMemory + llapNativeMemoryMb;
    Resource resource =
      isMap ?
          DAGUtils.getMapContainerGroupResource(conf, allLlapMemory, llapCpus) :
          DAGUtils.getReduceContainerGroupResource(conf);
    DAGAPI.ResourceProto perMapReduceResource = createResourceProto(resource);

    DAGAPI.ContainerConfigurationProto.Builder perMapReduceContainerConf =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(perMapReduceResource);
    setJavaOptsEnvironmentStr(conf, perMapReduceContainerConf);

    DAGAPI.ContainerGroupProto.Builder perMapReduceContainerGroup =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(groupName)
          .setContainerConfig(perMapReduceContainerConf.build())
          .setPriority(priority)
          .setContainerGroupConf(getContainerGroupConfProto(conf, 0, null));
    if (isMap && llapDaemonVertexProto != null) {
      List<DAGAPI.DaemonVertexProto> daemonVertexProtos = Collections.singletonList(llapDaemonVertexProto);
      perMapReduceContainerGroup.addAllDaemonVertices(daemonVertexProtos);
    }

    return perMapReduceContainerGroup.build();
  }

  private DAGAPI.ContainerGroupProto createPerVertexContainerGroupProto(
      Configuration conf, Vertex vertex) {
    int priority = vertex.getDistanceFromRoot() * 3;

    Resource resource =
      vertex.isMapVertex() ?
          DAGUtils.getMapContainerGroupResource(conf, 0, 0) :
          DAGUtils.getReduceContainerGroupResource(conf);
    DAGAPI.ResourceProto vertexResource = createResourceProto(resource);

    DAGAPI.ContainerConfigurationProto.Builder containerConfig =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(vertexResource);
    String javaOpts = vertex.getContainerJavaOpts();
    if (javaOpts != null) {
      containerConfig.setJavaOpts(javaOpts);
    }
    String environmentStr = vertex.getContainerEnvironment();
    if (environmentStr != null) {
      containerConfig.setEnvironmentStr(environmentStr);
    }

    DAGAPI.ContainerGroupProto perVertexContainerGroupProto =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(vertex.getName())
          .setContainerConfig(containerConfig.build())
          .setPriority(priority)
          .setContainerGroupConf(getContainerGroupConfProto(conf, 0, null))
          .build();

    return perVertexContainerGroupProto;
  }

  // if ALL_IN_ONE, then tezConf != null
  private DAGAPI.ConfigurationProto getContainerGroupConfProto(
      Configuration conf, int useDaemonShuffleHandler, TezConfiguration tezConf) {
    boolean combineTaskAttempts = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_COMBINE_TASKATTEMPTS);
    boolean containerReuse = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_REUSE);
    boolean mixTaskAttempts = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_MIX_TASKATTEMPTS);

    MR3ConfBuilder builder = new MR3ConfBuilder(false)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_COMBINE_TASKATTEMPTS(), combineTaskAttempts)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_REUSE(), containerReuse)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_MIX_TASKATTEMPTS(), mixTaskAttempts);

    builder.setInt(MR3Conf$.MODULE$.MR3_USE_DAEMON_SHUFFLEHANDLER(), useDaemonShuffleHandler);
    if (tezConf != null) {
      String serviceId = tezConf.get(
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      int port = tezConf.getInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
      builder.set(MR3Conf$.MODULE$.MR3_DAEMON_SHUFFLE_SERVICE_ID(), serviceId);
      builder.setInt(MR3Conf$.MODULE$.MR3_DAEMON_SHUFFLE_PORT(), port);
    }
    // if ALL_IN_ONE, then both MR3_DAEMON_SHUFFLE_SERVICE_ID and MR3_DAEMON_SHUFFLE_PORT are set in ContainerGroupConf

    String capacitySpecs = HiveConf.getVar(conf,
        HiveConf.ConfVars.MR3_DAG_QUEUE_CAPACITY_SPECS);
    if (capacitySpecs != null && !capacitySpecs.isEmpty()) {
      builder.set(MR3Conf$.MODULE$.MR3_DAG_QUEUE_CAPACITY_SPECS(), capacitySpecs);
    }

    MR3Conf containerGroupConf = builder.build();

    return Utils$.MODULE$.createMr3ConfProto(containerGroupConf);
  }

  private void setJavaOptsEnvironmentStr(
      Configuration conf,
      DAGAPI.ContainerConfigurationProto.Builder containerConf) {
    String javaOpts = DAGUtils.getContainerJavaOpts(conf);
    if (javaOpts != null) {
      containerConf.setJavaOpts(javaOpts);
    }

    String environmentStr = DAGUtils.getContainerEnvironment(conf);
    if (environmentStr != null) {
      containerConf.setEnvironmentStr(environmentStr);
    }
  }

  private DAGAPI.ResourceProto createResourceProto(Resource resource) {
    return
      DAGAPI.ResourceProto.newBuilder()
          .setMemoryMb(resource.getMemory())
          .setVirtualCores(resource.getVirtualCores())
          .setCoreDivisor(this.vcoresDivisor)
          .build();
  }
}
