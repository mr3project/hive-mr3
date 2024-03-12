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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DataSource;
import org.apache.hadoop.hive.ql.exec.mr3.dag.Edge;
import org.apache.hadoop.hive.ql.exec.mr3.dag.EdgeProperty;
import org.apache.hadoop.hive.ql.exec.mr3.dag.EntityDescriptor;
import org.apache.hadoop.hive.ql.exec.mr3.dag.GroupInputEdge;
import org.apache.hadoop.hive.ql.exec.mr3.dag.Vertex;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.tez.CustomPartitionEdge;
import org.apache.hadoop.hive.ql.exec.tez.CustomPartitionVertex;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.exec.tez.MapTezProcessor;
import org.apache.hadoop.hive.ql.exec.tez.MergeFileTezProcessor;
import org.apache.hadoop.hive.ql.exec.tez.NullMROutput;
import org.apache.hadoop.hive.ql.exec.tez.ReduceTezProcessor;
import org.apache.hadoop.hive.ql.exec.tez.TezConfigurationFactory;
import org.apache.hadoop.hive.ql.exec.tez.tools.TezMergedLogicalInput;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils.NullOutputCommitter;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapReduceMapWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import com.datamonad.mr3.api.common.MR3UncheckedException;
import com.datamonad.mr3.common.security.TokenCache;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.input.MultiMRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductConfig;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductEdgeManager;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * DAGUtils. DAGUtils is a collection of helper methods to convert
 * map and reduce work to tez vertices and edges. It handles configuration
 * objects, file localization and vertex/edge creation.
 */
public class DAGUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DAGUtils.class.getName());
  private static DAGUtils instance;

  private static final String MR3_DIR = "_mr3_scratch_dir";
  private static final int defaultAllInOneContainerMemoryMb = 1024;
  private static final int defaultAllInOneContainerVcores = 1;

  /**
   * Notifiers to synchronize resource localization across threads. If one thread is localizing
   * a file, other threads can wait on the corresponding notifier object instead of just sleeping
   * before re-checking HDFS. This is used just to avoid unnecesary waits; HDFS check still needs
   * to be performed to make sure the resource is there and matches the expected file.
   */
  private final ConcurrentHashMap<String, Object> copyNotifiers = new ConcurrentHashMap<>();

  /**
   * Singleton
   * @return instance of this class
   */
  public static DAGUtils getInstance() {
    if (instance == null) {
      instance = new DAGUtils();
    }
    return instance;
  }

  private DAGUtils() {
  }

  // return true if we should try to add credentials for accessing paths on HDFS
  // requirement: the user should explicitly set dfs.encryption.key.provider.uri in order to obtain credentials
  public boolean shouldAddPathsToCredentials(Configuration conf) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return false;
    }
    // dfs.encryption.key.provider.uri is deprecated and replaced by hadoop.security.key.provider.path
    String keyProvider = conf.get("dfs.encryption.key.provider.uri");
    if (keyProvider == null || keyProvider.equals("")) {
      keyProvider = conf.get("hadoop.security.key.provider.path");
    }
    if (keyProvider == null || keyProvider.equals("")) {
      return false;
    }
    return true;
  }

  /**
   * Set up credentials for the base work on secure clusters
   */
  public Set<Path> getPathsForCredentials(BaseWork work) {
    Set<Path> paths;
    if (work instanceof MapWork) {
      paths = getPathsForCredentialsMap((MapWork) work);
    } else {
      paths = new HashSet<Path>();
    }

    Set<URI> fileSinkUris = getFileSinkUrisForCredentials(work);
    for (URI uri : fileSinkUris) {
      paths.add(new Path(uri));
    }
    return paths;
  }

  private Set<Path> getPathsForCredentialsMap(MapWork mapWork) {
    Set<Path> paths = mapWork.getPathToAliases().keySet();
    if (LOG.isDebugEnabled() && !paths.isEmpty()) {
      for (Path path: paths) {
        LOG.debug("Marking MapWorker input Path as needing credentials: " + path);
      }
    }
    return paths;
  }

  private Set<URI> getFileSinkUrisForCredentials(BaseWork baseWork) {
    Set<URI> fileSinkUris = new HashSet<URI>();

    List<Node> topNodes = DagUtils.getTopNodes(baseWork);

    LOG.debug("Collecting file sink uris for {} topnodes: {}", baseWork.getClass(), topNodes);
    DagUtils.collectFileSinkUris(topNodes, fileSinkUris);

    if (LOG.isDebugEnabled()) {
      for (URI fileSinkUri: fileSinkUris) {
        LOG.debug("Marking {} output URI as needing credentials (filesink): {}",
            baseWork.getClass(), fileSinkUri);
      }
    }

    return fileSinkUris;
  }

  public void addPathsToCredentials(
          Credentials creds, Collection<Path> paths, Configuration conf) throws IOException {
    TokenCache.obtainTokensForFileSystems(creds, paths.toArray(new Path[paths.size()]), conf);
  }

  /**
   * Create a vertex from a given work object.
   *
   * @param conf JobConf to be used to this execution unit
   * @param work The instance of BaseWork representing the actual work to be performed
   * by this vertex.
   * @param mr3ScratchDir HDFS scratch dir for this execution unit.
   * @param fileSystem FS corresponding to scratchDir and LocalResources
   * @param ctx This query's context
   * @return Vertex
   */
  // we do not write anything to mr3ScratchDir, but still need it for the path to Plan
  @SuppressWarnings("deprecation")
  public Vertex createVertex(
      JobConf vertexJobConf, BaseWork work,
      Path mr3ScratchDir,
      boolean isFinal,
      VertexType vertexType, TezWork tezWork) throws Exception {

    Vertex vertex;
    // simply dispatch the call to the right method for the actual (sub-) type of BaseWork
    if (work instanceof MapWork) {
      vertex = createMapVertex(vertexJobConf, (MapWork) work, mr3ScratchDir, vertexType);
    } else if (work instanceof ReduceWork) {
      vertex = createReduceVertex(vertexJobConf, (ReduceWork) work, mr3ScratchDir);
    } else if (work instanceof MergeJoinWork) {
      vertex = createMergeJoinVertex(vertexJobConf, (MergeJoinWork) work, mr3ScratchDir, vertexType);

      // set VertexManagerPlugin if whether it's a cross product destination vertex
      List<String> crossProductSources = new ArrayList<>();
      for (BaseWork parentWork : tezWork.getParents(work)) {
        if (tezWork.getEdgeType(parentWork, work) == EdgeType.XPROD_EDGE) {
          crossProductSources.add(parentWork.getName());
        }
      }

      if (!crossProductSources.isEmpty()) {
        CartesianProductConfig cpConfig = new CartesianProductConfig(crossProductSources);
        org.apache.tez.dag.api.VertexManagerPluginDescriptor tezDescriptor =
            org.apache.tez.dag.api.VertexManagerPluginDescriptor
              .create(CartesianProductVertexManager.class.getName())
              .setUserPayload(cpConfig.toUserPayload(new TezConfiguration(vertexJobConf)));
        EntityDescriptor vmPlugin = MR3Utils.convertTezEntityDescriptor(tezDescriptor);
        vertex.setVertexManagerPlugin(vmPlugin);
        // parallelism shouldn't be set for cartesian product vertex
        LOG.info("Set VertexManager: CartesianProductVertexManager {}", vertex.getName());
      }
    } else if (work instanceof MapReduceMapWork) {
      vertex = createMapReduceMapVertex(vertexJobConf, (MapReduceMapWork)work);
    } else {
      // something is seriously wrong if this is happening
      throw new HiveException(ErrorMsg.GENERIC_ERROR.getErrorCodedMsg());
    }

    initializeStatsPublisher(vertexJobConf, work);

    final Class outputKlass;
    if (HiveOutputFormatImpl.class.getName().equals(vertexJobConf.get("mapred.output.format.class"))) {
      // Hive uses this output format, when it is going to write all its data through FS operator
      outputKlass = NullMROutput.class;
    } else {
      outputKlass = MROutput.class;
    }
    // final vertices need to have at least one output
    if (isFinal && !(work instanceof MapReduceMapWork)) {
      EntityDescriptor logicalOutputDescriptor = new EntityDescriptor(
          outputKlass.getName(),
          vertex.getProcessorDescriptorPayload());
      // no need to set OutputCommitter, Hive will handle moving temporary files to permanent locations
      vertex.addDataSink("out_" + work.getName(), logicalOutputDescriptor);
    }

    return vertex;
  }

  private void initializeStatsPublisher(JobConf jobConf, BaseWork work) throws Exception {
    if (work.isGatheringStats()) {
      StatsPublisher statsPublisher;
      StatsFactory factory = StatsFactory.newFactory(jobConf);
      if (factory != null) {
        StatsCollectionContext sCntxt = new StatsCollectionContext(jobConf);
        sCntxt.setStatsTmpDirs(Utilities.getStatsTmpDirs(work, jobConf));
        statsPublisher = factory.getStatsPublisher();
        if (!statsPublisher.init(sCntxt)) { // creating stats table if not exists
          if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw
                new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
          }
        }
      }
    }
  }

  private Vertex.VertexExecutionContext createVertexExecutionContext(BaseWork work) {
    if (work.getLlapMode()) {
      return Vertex.VertexExecutionContext.EXECUTE_IN_LLAP;
    }
    if (work.getUberMode()) {
      return Vertex.VertexExecutionContext.EXECUTE_IN_AM;
    }
    return Vertex.VertexExecutionContext.EXECUTE_IN_CONTAINER;
  }

  private Vertex createMergeJoinVertex(
      JobConf vertexJobConf, MergeJoinWork mergeJoinWork,
      Path mr3ScratchDir,
      VertexType vertexType) throws Exception {

    // jobConf updated
    Utilities.setMergeWork(vertexJobConf, mergeJoinWork, mr3ScratchDir, false);

    if (mergeJoinWork.getMainWork() instanceof MapWork) {
      List<BaseWork> mapWorkList = mergeJoinWork.getBaseWorkList();
      MapWork mapWork = (MapWork) (mergeJoinWork.getMainWork());
      Vertex mergeVx = createMapVertex(vertexJobConf, mapWork, mr3ScratchDir, vertexType);

      vertexJobConf.setClass("mapred.input.format.class", HiveInputFormat.class, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set
      // to false when using this plug-in to avoid getting a serialized event at run-time.
      vertexJobConf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
      for (int i = 0; i < mapWorkList.size(); i++) {
        mapWork = (MapWork) (mapWorkList.get(i));
        vertexJobConf.set(org.apache.hadoop.hive.ql.exec.tez.DagUtils.TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX,
            mapWork.getName());
        vertexJobConf.set(Utilities.INPUT_NAME, mapWork.getName());
        LOG.info("Going through each work and adding MultiMRInput");

        org.apache.tez.dag.api.DataSourceDescriptor dataSource=
            MultiMRInput.createConfigBuilder(vertexJobConf, HiveInputFormat.class).build();
        DataSource mr3DataSource = MR3Utils.convertTezDataSourceDescriptor(dataSource);
        mergeVx.addDataSource(mapWork.getName(), mr3DataSource);
      }

      // To be populated for SMB joins only for all the small tables
      Map<String, Integer> inputToBucketMap = new HashMap<>();
      if (mergeJoinWork.getMergeJoinOperator().getParentOperators().size() == 1
              && mergeJoinWork.getMergeJoinOperator().getOpTraits() != null) {
        // This is an SMB join.
        for (BaseWork work : mapWorkList) {
          MapWork mw = (MapWork) work;
          Map<String, Operator<?>> aliasToWork = mw.getAliasToWork();
          Preconditions.checkState(aliasToWork.size() == 1,
                  "More than 1 alias in SMB mapwork");
          inputToBucketMap.put(mw.getName(), mw.getWorks().get(0).getOpTraits().getNumBuckets());
        }
      }

      String vertexManagerPluginClassName = CustomPartitionVertex.class.getName();
      // the +1 to the size is because of the main work.
      CustomVertexConfiguration vertexConf =
          new CustomVertexConfiguration(mergeJoinWork.getMergeJoinOperator().getConf()
              .getNumBuckets(), vertexType, mergeJoinWork.getBigTableAlias(),
              mapWorkList.size() + 1, inputToBucketMap);
      ByteString userPayload = MR3Utils.createUserPayloadFromVertexConf(vertexConf);
      EntityDescriptor vertexManagerPluginDescriptor =
          new EntityDescriptor(vertexManagerPluginClassName, userPayload);
      mergeVx.setVertexManagerPlugin(vertexManagerPluginDescriptor);
      LOG.info("Set VertexManager: CustomPartitionVertex(MergeJoin) {}", mergeVx.getName());

      return mergeVx;
    } else {
      Vertex mergeVx =
          createReduceVertex(vertexJobConf, (ReduceWork) mergeJoinWork.getMainWork(), mr3ScratchDir);
      return mergeVx;
    }
  }

  /*
   * Helper function to create Vertex from MapWork.
   */
  private Vertex createMapVertex(
      JobConf vertexJobConf, MapWork mapWork,
      Path mr3ScratchDir,
      VertexType vertexType) throws Exception {

    // set up the operator plan
    Utilities.cacheMapWork(vertexJobConf, mapWork, mr3ScratchDir);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(vertexJobConf, mapWork);

    boolean groupSplitsInInputInitializer;  // use tez to combine splits???
    org.apache.tez.dag.api.DataSourceDescriptor dataSource;
    int numTasks;

    @SuppressWarnings("rawtypes")
    Class inputFormatClass = vertexJobConf.getClass("mapred.input.format.class",
        InputFormat.class);

    boolean vertexHasCustomInput = VertexType.isCustomInputType(vertexType);
    LOG.info("Vertex has custom input? " + vertexHasCustomInput);
    if (vertexHasCustomInput) {
      groupSplitsInInputInitializer = false;
      // grouping happens in execution phase. The input payload should not enable grouping here,
      // it will be enabled in the CustomVertex.
      inputFormatClass = HiveInputFormat.class;
      vertexJobConf.setClass("mapred.input.format.class", HiveInputFormat.class, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set to false when using
      // this plug-in to avoid getting a serialized event at run-time.
      vertexJobConf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
    } else {
      // we'll set up tez to combine spits for us iff the input format
      // is HiveInputFormat
      if (inputFormatClass == HiveInputFormat.class) {
        groupSplitsInInputInitializer = true;
      } else {
        groupSplitsInInputInitializer = false;
      }
    }

    if (mapWork instanceof MergeFileWork) {
      Path outputPath = ((MergeFileWork) mapWork).getOutputDir();
      // prepare the tmp output directory. The output tmp directory should
      // exist before jobClose (before renaming after job completion)
      Path tempOutPath = Utilities.toTempPath(outputPath);
      try {
        FileSystem tmpOutFS = tempOutPath.getFileSystem(vertexJobConf);
        if (!tmpOutFS.exists(tempOutPath)) {
          tmpOutFS.mkdirs(tempOutPath);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Can't make path " + outputPath + " : " + e.getMessage(), e);
      }
    }

    // remember mapping of plan to input
    vertexJobConf.set(Utilities.INPUT_NAME, mapWork.getName());
    if (HiveConf.getBoolVar(vertexJobConf, ConfVars.HIVE_AM_SPLIT_GENERATION)) {

      // set up the operator plan. (before setting up splits on the AM)
      Utilities.setMapWork(vertexJobConf, mapWork, mr3ScratchDir, false);

      // if we're generating the splits in the AM, we just need to set
      // the correct plugin.
      if (groupSplitsInInputInitializer) {
        // Not setting a payload, since the MRInput payload is the same and can be accessed.
        InputInitializerDescriptor descriptor = InputInitializerDescriptor.create(
            HiveSplitGenerator.class.getName());
        dataSource = MRInputLegacy.createConfigBuilder(vertexJobConf, inputFormatClass).groupSplits(true)
            .setCustomInitializerDescriptor(descriptor).build();
      } else {
        // Not HiveInputFormat, or a custom VertexManager will take care of grouping splits
        if (vertexHasCustomInput && vertexType == VertexType.MULTI_INPUT_UNINITIALIZED_EDGES) {
          // SMB Join.
          dataSource =
              MultiMRInput.createConfigBuilder(vertexJobConf, inputFormatClass).groupSplits(false).build();
        } else {
          dataSource =
              MRInputLegacy.createConfigBuilder(vertexJobConf, inputFormatClass).groupSplits(false).build();
        }
      }
      numTasks = -1;  // to be decided at runtime
    } else {
      // Setup client side split generation.

      // we need to set this, because with HS2 and client side split
      // generation we end up not finding the map work. This is
      // because of thread local madness (tez split generation is
      // multi-threaded - HS2 plan cache uses thread locals). Setting
      // VECTOR_MODE/USE_VECTORIZED_INPUT_FILE_FORMAT causes the split gen code to use the conf instead
      // of the map work.
      vertexJobConf.setBoolean(Utilities.VECTOR_MODE, mapWork.getVectorMode());
      vertexJobConf.setBoolean(Utilities.USE_VECTORIZED_INPUT_FILE_FORMAT, mapWork.getUseVectorizedInputFileFormat());

      InputSplitInfo inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(vertexJobConf, false, 0);
      InputInitializerDescriptor descriptor = InputInitializerDescriptor.create(MRInputSplitDistributor.class.getName());
      InputDescriptor inputDescriptor = InputDescriptor.create(MRInputLegacy.class.getName())
              .setUserPayload(UserPayload
                      .create(MRRuntimeProtos.MRInputUserPayloadProto.newBuilder()
                              .setConfigurationBytes(TezUtils.createByteStringFromConf(vertexJobConf))
                              .setSplits(inputSplitInfo.getSplitsProto()).build().toByteString()
                              .asReadOnlyByteBuffer()));

      dataSource = DataSourceDescriptor.create(inputDescriptor, descriptor, null);
      numTasks = inputSplitInfo.getNumTasks();

      // set up the operator plan. (after generating splits - that changes configs)
      Utilities.setMapWork(vertexJobConf, mapWork, mr3ScratchDir, false);
    }

    String procClassName = MapTezProcessor.class.getName();
    if (mapWork instanceof MergeFileWork) {
      procClassName = MergeFileTezProcessor.class.getName();
    }

    ByteString userPayload = TezUtils.createByteStringFromConf(vertexJobConf);
    EntityDescriptor processorDescriptor = new EntityDescriptor(procClassName, userPayload);

    Resource taskResource = getMapTaskResource(vertexJobConf);
    String containerEnvironment = getContainerEnvironment(vertexJobConf);
    String containerJavaOpts = getContainerJavaOpts(vertexJobConf);

    Vertex.VertexExecutionContext executionContext = createVertexExecutionContext(mapWork);
    Vertex map = Vertex.create(
        mapWork.getName(), processorDescriptor,
        numTasks,
        taskResource, containerEnvironment, containerJavaOpts, true, executionContext);

    assert mapWork.getAliasToWork().keySet().size() == 1;

    // Add the actual source input
    String alias = mapWork.getAliasToWork().keySet().iterator().next();
    DataSource mr3DataSource = MR3Utils.convertTezDataSourceDescriptor(dataSource);
    map.addDataSource(alias, mr3DataSource);

    return map;
  }

  /*
   * Helper function to create Vertex for given ReduceWork.
   */
  private Vertex createReduceVertex(
      JobConf vertexJobConf, ReduceWork reduceWork,
      Path mr3ScratchDir) throws Exception {

    // set up operator plan
    vertexJobConf.set(Utilities.INPUT_NAME, reduceWork.getName());
    Utilities.setReduceWork(vertexJobConf, reduceWork, mr3ScratchDir, false);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(vertexJobConf, reduceWork);

    EntityDescriptor processorDescriptor = new EntityDescriptor(
        ReduceTezProcessor.class.getName(),
        TezUtils.createByteStringFromConf(vertexJobConf));

    Resource taskResource = getReduceTaskResource(vertexJobConf);
    String containerEnvironment = getContainerEnvironment(vertexJobConf);
    String containerJavaOpts = getContainerJavaOpts(vertexJobConf);

    Vertex.VertexExecutionContext executionContext = createVertexExecutionContext(reduceWork);
    Vertex reducer = Vertex.create(
        reduceWork.getName(), processorDescriptor,
        reduceWork.isAutoReduceParallelism() ? reduceWork.getMaxReduceTasks() : reduceWork.getNumReduceTasks(),
        taskResource, containerEnvironment, containerJavaOpts, false, executionContext);

    return reducer;
  }

  private Vertex createMapReduceMapVertex(JobConf vertexJobConf, MapReduceMapWork mapReduceMapWork) throws Exception {
    vertexJobConf.set(Utilities.INPUT_NAME, mapReduceMapWork.getName());
    ByteString jobConfByteString = TezUtils.createByteStringFromConf(vertexJobConf);

    EntityDescriptor processorDescriptor = new EntityDescriptor(
        MRMapProcessor.class.getName(), jobConfByteString);
    Resource taskResource = getMapTaskResource(vertexJobConf);
    String containerEnvironment = getContainerEnvironment(vertexJobConf);
    String containerJavaOpts = getContainerJavaOpts(vertexJobConf);
    Vertex.VertexExecutionContext executionContext = createVertexExecutionContext(mapReduceMapWork);

    Vertex vertex = Vertex.create(mapReduceMapWork.getName(), processorDescriptor, -1, taskResource,
        containerEnvironment, containerJavaOpts, true, executionContext);

    final Class inputFormatClass;
    if (vertexJobConf.getBoolean("mapred.mapper.new-api", false)) {
      inputFormatClass = vertexJobConf.getClass("mapreduce.job.inputformat.class", InputFormat.class);
      LOG.info("MapReduceMapVertex uses new MapReduce API: {} {}", mapReduceMapWork.getName(), inputFormatClass);
    } else {
      inputFormatClass = vertexJobConf.getClass("mapred.input.format.class", InputFormat.class);
      LOG.info("MapReduceMapVertex uses old MapReduce API: {} {}", mapReduceMapWork.getName(), inputFormatClass);
    }
    DataSourceDescriptor dataSource =
        MRInputLegacy.createConfigBuilder(vertexJobConf, inputFormatClass).groupSplits(false).build();
    DataSource mr3DataSource = MR3Utils.convertTezDataSourceDescriptor(dataSource);
    vertex.addDataSource("in_" + mapReduceMapWork.getName(), mr3DataSource);

    EntityDescriptor logicalOutputDescriptor = new EntityDescriptor(
        MROutputLegacy.class.getName(),
        jobConfByteString);
    EntityDescriptor outputCommitterDescriptor = new EntityDescriptor(
        MROutputCommitter.class.getName(),
        jobConfByteString);
    vertex.addDataSink("out_" + mapReduceMapWork.getName(), logicalOutputDescriptor, outputCommitterDescriptor);

    return vertex;
  }

  /**
   * Creates and initializes the JobConf object for a given BaseWork object.
   *
   * @param conf Any configurations in conf will be copied to the resulting new JobConf object.
   * @param work BaseWork will be used to populate the configuration object.
   * @return JobConf new configuration object
   */
  public JobConf initializeVertexConf(JobConf jobConf, Context context, BaseWork work) {
    // simply dispatch the call to the right method for the actual (sub-) type of BaseWork.
    if (work instanceof MapWork) {
      return initializeMapVertexConf(jobConf, context, (MapWork)work);
    } else if (work instanceof ReduceWork) {
      return initializeReduceVertexConf(jobConf, context, (ReduceWork)work);
    } else if (work instanceof MergeJoinWork) {
      return initializeMergeJoinVertexConf(jobConf, context, (MergeJoinWork) work);
    } else if (work instanceof MapReduceMapWork) {
      return initializeMapReduceMapVertexConf(jobConf, (MapReduceMapWork) work);
    } else {
      assert false;
      return null;
    }
  }

  private JobConf initializeMapReduceMapVertexConf(JobConf jobConf, MapReduceMapWork work) {
    return work.configureVertexConf(jobConf);   // ignore jobConf and return work.jobConf
  }

  private JobConf initializeMergeJoinVertexConf(JobConf jobConf, Context context, MergeJoinWork work) {
    if (work.getMainWork() instanceof MapWork) {
      return initializeMapVertexConf(jobConf, context, (MapWork) (work.getMainWork()));
    } else {
      return initializeReduceVertexConf(jobConf, context, (ReduceWork) (work.getMainWork()));
    }
  }

  /*
   * Helper function to create JobConf for specific ReduceWork.
   */
  private JobConf initializeReduceVertexConf(JobConf baseConf, Context context, ReduceWork reduceWork) {
    JobConf jobConf = new JobConf(baseConf);

    jobConf.set(Operator.CONTEXT_NAME_KEY, reduceWork.getName());

    // Is this required ?
    jobConf.set("mapred.reducer.class", ExecReducer.class.getName());

    jobConf.setBoolean(org.apache.hadoop.mapreduce.MRJobConfig.REDUCE_SPECULATIVE, false);

    return jobConf;
  }

  /*
   * Creates the configuration object necessary to run a specific vertex from
   * map work. This includes input formats, input processor, etc.
   */
  private JobConf initializeMapVertexConf(JobConf baseConf, Context context, MapWork mapWork) {
    JobConf jobConf = new JobConf(baseConf);

    jobConf.set(Operator.CONTEXT_NAME_KEY, mapWork.getName());

    if (mapWork.getNumMapTasks() != null) {
      // Is this required ?
      jobConf.setInt(MRJobConfig.NUM_MAPS, mapWork.getNumMapTasks().intValue());
    }

    if (mapWork.getMaxSplitSize() != null) {
      HiveConf.setLongVar(jobConf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE,
          mapWork.getMaxSplitSize().longValue());
    }

    if (mapWork.getMinSplitSize() != null) {
      HiveConf.setLongVar(jobConf, HiveConf.ConfVars.MAPREDMINSPLITSIZE,
          mapWork.getMinSplitSize().longValue());
    }

    if (mapWork.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(jobConf, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERNODE,
          mapWork.getMinSplitSizePerNode().longValue());
    }

    if (mapWork.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(jobConf, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERRACK,
          mapWork.getMinSplitSizePerRack().longValue());
    }

    Utilities.setInputAttributes(jobConf, mapWork);

    String inpFormat = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVETEZINPUTFORMAT);

    if (mapWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    if (mapWork.getDummyTableScan()) {
      // hive input format doesn't handle the special condition of no paths + 1
      // split correctly.
      inpFormat = CombineHiveInputFormat.class.getName();
    }

    jobConf.set(org.apache.hadoop.hive.ql.exec.tez.DagUtils.TEZ_TMP_DIR_KEY,
        context.getMRTmpPath().toUri().toString());
    jobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    jobConf.set("mapred.input.format.class", inpFormat);

    if (mapWork instanceof MergeFileWork) {
      MergeFileWork mfWork = (MergeFileWork) mapWork;
      // This mapper class is used for serialization/deserialization of merge file work.
      jobConf.set("mapred.mapper.class", MergeFileMapper.class.getName());
      jobConf.set("mapred.input.format.class", mfWork.getInputformat());
      jobConf.setClass("mapred.output.format.class", MergeFileOutputFormat.class,
          FileOutputFormat.class);
    }

    return jobConf;
  }

  /**
   * Given a Vertex group and a vertex createEdge will create an
   * Edge between them.
   *
   * @param group The parent VertexGroup
   * @param parentJobConf Jobconf of one of the parent vertices in VertexGroup
   * @param edgeProp the edge property of connection between the two
   * endpoints.
   */
  @SuppressWarnings("rawtypes")
  public GroupInputEdge createGroupInputEdge(
      JobConf parentJobConf, Vertex destVertex,
      TezEdgeProperty edgeProp,
      BaseWork work, TezWork tezWork)
    throws IOException {

    LOG.info("Creating GroupInputEdge to " + destVertex.getName());

    Class mergeInputClass;
    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
    case BROADCAST_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;
    case CUSTOM_EDGE: {
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;

      // update VertexManagerPlugin of destVertex
      String vertexManagerClassName = CustomPartitionVertex.class.getName();
      int numBuckets = edgeProp.getNumBuckets();
      VertexType vertexType = tezWork.getVertexType(work);
      CustomVertexConfiguration vertexConf = new CustomVertexConfiguration(numBuckets, vertexType);
      ByteString userPayload = MR3Utils.createUserPayloadFromVertexConf(vertexConf);
      EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(
          vertexManagerClassName, userPayload);
      destVertex.setVertexManagerPlugin(vertexManagerPluginDescriptor);
      LOG.info("Set VertexManager: CustomPartitionVertex(GroupInputEdge, CUSTOM_EDGE) {}", destVertex.getName());
      break;
    }

    case CUSTOM_SIMPLE_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case ONE_TO_ONE_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case XPROD_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case SIMPLE_EDGE:
      setupAutoReducerParallelism(edgeProp, destVertex, parentJobConf);
      // fall through

    default:
      mergeInputClass = TezMergedLogicalInput.class;
      break;
    }

    org.apache.tez.dag.api.EdgeProperty ep = createTezEdgeProperty(edgeProp, parentJobConf, work, tezWork);
    EdgeProperty edgeProperty = MR3Utils.convertTezEdgeProperty(ep);
    if (edgeProp.isFixed()) {   // access edgeProp directly
      LOG.info("Set VertexManager setting FIXED: GroupInputEdge to {}, {}",
          destVertex.getName(), edgeProp.getEdgeType());
      edgeProperty.setFixed();
    }
    EntityDescriptor mergedInputDescriptor = new EntityDescriptor(mergeInputClass.getName(), null);

    return new GroupInputEdge(destVertex, edgeProperty, mergedInputDescriptor);
  }

  /**
   * Given two vertices and the configuration for the source vertex, createEdge
   * will create an Edge object that connects the two.
   *
   * @param vConf JobConf of the first (source) vertex
   * @param v The first vertex (source)
   * @param w The second vertex (sink)
   * @return
   */
  public Edge createEdge(JobConf vConf, Vertex v, Vertex w, TezEdgeProperty edgeProp,
      BaseWork work, TezWork tezWork)
    throws IOException {

    switch(edgeProp.getEdgeType()) {
    case CUSTOM_EDGE: {
      String vertexManagerClassName = CustomPartitionVertex.class.getName();

      int numBuckets = edgeProp.getNumBuckets();
      VertexType vertexType = tezWork.getVertexType(work);
      CustomVertexConfiguration vertexConf = new CustomVertexConfiguration(numBuckets, vertexType);
      ByteString userPayload = MR3Utils.createUserPayloadFromVertexConf(vertexConf);
      EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(
          vertexManagerClassName, userPayload);

      w.setVertexManagerPlugin(vertexManagerPluginDescriptor);
      LOG.info("Set VertexManager: CustomPartitionVertex(Edge, CUSTOM_EDGE) {}", w.getName());
      break;
    }
    case XPROD_EDGE:
      break;

    case SIMPLE_EDGE: {
      setupAutoReducerParallelism(edgeProp, w, vConf);
      break;
    }
    case CUSTOM_SIMPLE_EDGE: {
      setupQuickStart(edgeProp, w, vConf);
      break;
    }

    default:
      // nothing
    }

    org.apache.tez.dag.api.EdgeProperty ep = createTezEdgeProperty(edgeProp, vConf, work, tezWork);
    EdgeProperty edgeProperty = MR3Utils.convertTezEdgeProperty(ep);
    if (edgeProp.isFixed()) {   // access edgeProp directly
      LOG.info("Set VertexManager setting FIXED: Edge from {} to {}, {}",
          v.getName(), w.getName(), edgeProp.getEdgeType());
      edgeProperty.setFixed();
    }

    return new Edge(v, w, edgeProperty);
  }

  /*
   * Helper function to create an edge property from an edge type.
   */
  private org.apache.tez.dag.api.EdgeProperty createTezEdgeProperty(
        TezEdgeProperty edgeProp,
        Configuration conf,
        BaseWork work, TezWork tezWork) throws IOException {
    MRHelpers.translateMRConfToTez(conf);
    String keyClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    String valClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    String partitionerClassName = conf.get("mapred.partitioner.class");
    Map<String, String> partitionerConf;

    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
    case BROADCAST_EDGE:
      UnorderedKVEdgeConfig et1Conf = UnorderedKVEdgeConfig
          .newBuilder(keyClass, valClass)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et1Conf.createDefaultBroadcastEdgeProperty();
    case CUSTOM_EDGE:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      UnorderedPartitionedKVEdgeConfig et2Conf = UnorderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      EdgeManagerPluginDescriptor edgeDesc =
          EdgeManagerPluginDescriptor.create(CustomPartitionEdge.class.getName());
      CustomEdgeConfiguration edgeConf =
          new CustomEdgeConfiguration(edgeProp.getNumBuckets(), null);
      DataOutputBuffer dob = new DataOutputBuffer();
      edgeConf.write(dob);
      byte[] userPayload = dob.getData();
      edgeDesc.setUserPayload(UserPayload.create(ByteBuffer.wrap(userPayload)));
      return et2Conf.createDefaultCustomEdgeProperty(edgeDesc);
    case CUSTOM_SIMPLE_EDGE:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      UnorderedPartitionedKVEdgeConfig.Builder et3Conf = UnorderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null);
      if (edgeProp.getBufferSize() != null) {
        et3Conf.setAdditionalConfiguration(
            TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
            edgeProp.getBufferSize().toString());
      }
      return et3Conf.build().createDefaultEdgeProperty();
    case ONE_TO_ONE_EDGE:
      UnorderedKVEdgeConfig et4Conf = UnorderedKVEdgeConfig
          .newBuilder(keyClass, valClass)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et4Conf.createDefaultOneToOneEdgeProperty();
    case XPROD_EDGE:
      EdgeManagerPluginDescriptor edgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(CartesianProductEdgeManager.class.getName());
      List<String> crossProductSources = new ArrayList<>();
      for (BaseWork parentWork : tezWork.getParents(work)) {
        if (EdgeType.XPROD_EDGE == tezWork.getEdgeType(parentWork, work)) {
          crossProductSources.add(parentWork.getName());
        }
      }
      CartesianProductConfig cpConfig = new CartesianProductConfig(crossProductSources);
      edgeManagerDescriptor.setUserPayload(cpConfig.toUserPayload(new TezConfiguration(conf)));
      UnorderedPartitionedKVEdgeConfig cpEdgeConf =
        UnorderedPartitionedKVEdgeConfig.newBuilder(keyClass, valClass,
          ValueHashPartitioner.class.getName())
            .setFromConfiguration(conf)
            .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
            .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
            .build();
      return cpEdgeConf.createDefaultCustomEdgeProperty(edgeManagerDescriptor);
    case SIMPLE_EDGE:
      // fallthrough
    default:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      OrderedPartitionedKVEdgeConfig et5Conf = OrderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(),
              TezBytesComparator.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et5Conf.createDefaultEdgeProperty();
    }
  }

  public static class ValueHashPartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      return (value.hashCode() & 2147483647) % numPartitions;
    }
  }

  /**
   * Utility method to create a stripped down configuration for the MR partitioner.
   *
   * @param partitionerClassName
   *          the real MR partitioner class name
   * @param baseConf
   *          a base configuration to extract relevant properties
   * @return
   */
  private Map<String, String> createPartitionerConf(String partitionerClassName,
      Configuration baseConf) {
    Map<String, String> partitionerConf = new HashMap<String, String>();
    partitionerConf.put("mapred.partitioner.class", partitionerClassName);
    if (baseConf.get("mapreduce.totalorderpartitioner.path") != null) {
      partitionerConf.put("mapreduce.totalorderpartitioner.path",
      baseConf.get("mapreduce.totalorderpartitioner.path"));
    }
    return partitionerConf;
  }

  public static Resource getMapTaskResource(Configuration conf) {
    return getResource(conf,
        HiveConf.ConfVars.MR3_MAP_TASK_MEMORY_MB,
        MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        HiveConf.ConfVars.MR3_MAP_TASK_VCORES,
        MRJobConfig.MAP_CPU_VCORES, MRJobConfig.DEFAULT_MAP_CPU_VCORES);
  }

  public static Resource getReduceTaskResource(Configuration conf) {
    return getResource(conf,
        HiveConf.ConfVars.MR3_REDUCE_TASK_MEMORY_MB,
        MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
        HiveConf.ConfVars.MR3_REDUCE_TASK_VCORES,
        MRJobConfig.REDUCE_CPU_VCORES, MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
  }

  public static Resource getMapContainerGroupResource(Configuration conf, int llapMemory, int llapCpus) {
    Resource resource = getResource(conf,
        ConfVars.MR3_MAP_CONTAINERGROUP_MEMORY_MB,
        MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB,
        ConfVars.MR3_MAP_CONTAINERGROUP_VCORES,
        MRJobConfig.MAP_CPU_VCORES, MRJobConfig.DEFAULT_MAP_CPU_VCORES);

    return Resource.newInstance(
      resource.getMemory() + llapMemory, resource.getVirtualCores() + llapCpus);
  }

  public static Resource getReduceContainerGroupResource(Configuration conf) {
    return getResource(conf,
        HiveConf.ConfVars.MR3_REDUCE_CONTAINERGROUP_MEMORY_MB,
        MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig.DEFAULT_REDUCE_MEMORY_MB,
        HiveConf.ConfVars.MR3_REDUCE_CONTAINERGROUP_VCORES,
        MRJobConfig.REDUCE_CPU_VCORES, MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
  }

  public static Resource getAllInOneContainerGroupResource(Configuration conf, int allLlapMemory, int llapCpus) {
    int memory = HiveConf.getIntVar(conf, ConfVars.MR3_ALLINONE_CONTAINERGROUP_MEMORY_MB);
    if (memory <= 0) {
      memory = defaultAllInOneContainerMemoryMb;
    }
    int cpus = HiveConf.getIntVar(conf, ConfVars.MR3_ALLINONE_CONTAINERGROUP_VCORES);
    if (cpus <= 0) {
      cpus = defaultAllInOneContainerVcores;
    }
    return Resource.newInstance(memory + allLlapMemory, cpus + llapCpus);
  }

  private static Resource getResource(
      Configuration conf,
      HiveConf.ConfVars sizeKey, String mrSizeKey, int mrSizeDefault,
      HiveConf.ConfVars coresKey, String mrCoresKey, int mrCoresDefault) {
    int memory = HiveConf.getIntVar(conf, sizeKey);
    if (memory < 0) {   // Task memory of 0 is allowed in hive-site.xml
      memory = conf.getInt(mrSizeKey, mrSizeDefault);
    }
    // TODO: memory can still be < 0, e.g., if both sizeKey and mrSizeKey are set to -1
    int cpus = HiveConf.getIntVar(conf, coresKey);
    if (cpus < 0) {     // Task cpus of 0 is allowed in hive-site.xml
      cpus = conf.getInt(mrCoresKey, mrCoresDefault);
    }
    // TODO: cpus can still be < 0, e.g., if both coresKey and mrCoresKey are set to -1
    return Resource.newInstance(memory, cpus);
  }

  @Nullable
  public static String getContainerEnvironment(Configuration conf) {
    String envString = HiveConf.getVar(conf, HiveConf.ConfVars.MR3_CONTAINER_ENV);

    // We do not need to further adjust envString because MR3 has its own configuration key
    // (MR3Conf.MR3_CONTAINER_LAUNCH_ENV, which is added to envString. For the user, it suffices to set
    // HiveConf.MR3_CONTAINER_ENV and MR3Conf.MR3_CONTAINER_LAUNCH_ENV.
    // Note that HiveConf.MR3_CONTAINER_ENV takes precedence over MR3Conf.MR3_CONTAINER_LAUNCH_ENV.
    // Cf. ContainerGroup.getEnvironment() in MR3

    return envString;
  }

  @Nullable
  public static String getContainerJavaOpts(Configuration conf) {
    String javaOpts = HiveConf.getVar(conf, HiveConf.ConfVars.MR3_CONTAINER_JAVA_OPTS);

    // We do not need to calculate logging level here because MR3 appends internally (in
    // ContainerGroup.createContainerGroup()) logging level to javaOpts specified by
    // MR3Conf.MR3_CONTAINER_LOG_LEVEL. For the user, it suffices to set logging level in mr3-site.xml.

    // We do not need to further adjust javaOpts because MR3 has its own configuration key
    // (MR3Conf.MR3_CONTAINER_LAUNCH_CMD_OPTS) which is prepended to ContainerGroup's javaOpts. For the user,
    // it suffices to set HiveConf.MR3_CONTAINER_JAVA_OPTS and MR3Conf.MR3_CONTAINER_LAUNCH_CMD_OPTS.
    // Note that HiveConf.ConfVars.MR3_CONTAINER_JAVA_OPTS takes precedence over MR3Conf.MR3_CONTAINER_LAUNCH_CMD_OPTS.
    // Cf. ContainerGroup.getRawOptionEnvLocalResources() in MR3

    return javaOpts;
  }

  /**
   * Primarily used because all LocalResource utilities return List[LocalResources].
   * MR3Client interface uses Map<String, LocalResources>, thus the reason for this utility
   */
  public Map<String, LocalResource> convertLocalResourceListToMap(List<LocalResource> localResourceList) {
    Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
    for ( LocalResource lr: localResourceList ) {
      localResourceMap.put(getBaseName(lr), lr);
    }
    return localResourceMap;
  }

  /*
   * Helper method to create a yarn local resource.
   */
  private LocalResource createLocalResource(FileSystem remoteFs, Path file,
      LocalResourceType type, LocalResourceVisibility visibility) throws IOException {

    final FileStatus fstat = remoteFs.getFileStatus(file);

    URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LOG.info("Resource modification time: " + resourceModificationTime + " for " + file);

    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);

    return lr;
  }

  /**
   * @param conf
   * @return path to destination directory on hdfs
   * @throws LoginException if we are unable to figure user information
   * @throws IOException when any dfs operation fails.
   */
  @SuppressWarnings("deprecation")
  public Path getDefaultDestDir(Configuration conf) throws LoginException, IOException {
    UserGroupInformation ugi = Utils.getUGI();
    String userName = ugi.getShortUserName();
    String userPathStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_USER_INSTALL_DIR);
    Path userPath = new Path(userPathStr);
    FileSystem fs = userPath.getFileSystem(conf);

    Path hdfsDirPath = new Path(userPathStr, userName);

    try {
      FileStatus fstatus = fs.getFileStatus(hdfsDirPath);
      if (!fstatus.isDir()) {
        throw new IOException(ErrorMsg.INVALID_DIR.format(hdfsDirPath.toString()));
      }
    } catch (FileNotFoundException e) {
      // directory does not exist, create it
      fs.mkdirs(hdfsDirPath);
    }

    Path retPath = new Path(hdfsDirPath.toString(), ".mr3hiveJars");

    fs.mkdirs(retPath);
    return retPath;
  }

  /**
   * Change in HIVEAUXJARS should result in a restart of hive, thus is added to
   * MR3 Sessions's init LocalResources for all tasks to use.
   * @param conf
   * @return
     */
  public String[] getSessionInitJars(Configuration conf) throws URISyntaxException  {
    boolean localizeSessionJars = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MR3_LOCALIZE_SESSION_JARS);
    if (localizeSessionJars) {
      String execjar = getExecJarPathLocal();
      String auxjars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      // need to localize the hive-exec jars and hive.aux.jars
      // we need the directory on hdfs to which we shall put all these files
      return (execjar + "," + auxjars).split(",");
    } else {
      LOG.info("Skipping localizing initial session jars");
      return new String[0];
    }
  }

  /**
   * Localizes files, archives and jars the user has instructed us
   * to provide on the cluster as resources for execution.
   *
   * @param conf
   * @return List<LocalResource> local resources to add to execution
   * @throws IOException when hdfs operation fails
   * @throws LoginException when getDefaultDestDir fails with the same exception
   */
  public List<LocalResource> localizeTempFilesFromConf(
      Path hdfsDirPathStr, Configuration conf) throws IOException, LoginException {
    List<LocalResource> tmpResources = new ArrayList<LocalResource>();

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEADDFILESUSEHDFSLOCATION)) {
      // reference HDFS based resource directly, to use distribute cache efficiently.
      addHdfsResource(conf, tmpResources, LocalResourceType.FILE, getHdfsTempFilesFromConf(conf));
      // local resources are session based.
      addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.FILE, getLocalTempFilesFromConf(conf));
    } else {
      // all resources including HDFS are session based.
      addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.FILE, getTempFilesFromConf(conf));
    }

    addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.ARCHIVE,
        getTempArchivesFromConf(conf));
    return tmpResources;
  }

  private void addHdfsResource(Configuration conf, List<LocalResource> tmpResources,
                               LocalResourceType type, String[] files) throws IOException {
    for (String file: files) {
      if (StringUtils.isNotBlank(file)) {
        Path dest = new Path(file);
        FileSystem destFS = dest.getFileSystem(conf);
        LocalResource localResource = createLocalResource(destFS, dest, type,
            LocalResourceVisibility.PRIVATE);
        tmpResources.add(localResource);
      }
    }
  }

  private static String[] getHdfsTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getHdfsResourceFiles(conf, SessionState.ResourceType.FILE);
    String addedJars = Utilities.getHdfsResourceFiles(conf, SessionState.ResourceType.JAR);
    String allFiles = addedJars + "," + addedFiles;
    return allFiles.split(",");
  }

  private static String[] getLocalTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getLocalResourceFiles(conf, SessionState.ResourceType.FILE);
    String addedJars = Utilities.getLocalResourceFiles(conf, SessionState.ResourceType.JAR);
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
    String reloadableAuxJars = SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars();
    String allFiles =
        HiveStringUtils.joinIgnoringEmpty(new String[]{auxJars, reloadableAuxJars, addedJars, addedFiles}, ',');
    return allFiles.split(",");
  }

  private String[] getTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }
    // do not add HiveConf.ConfVars.HIVEAUXJARS here which is added in getSessionInitJars()
    String reloadableAuxJars = SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars();

    // need to localize the additional jars and files
    // we need the directory on hdfs to which we shall put all these files
    String allFiles =
        HiveStringUtils.joinIgnoringEmpty(new String[]{reloadableAuxJars, addedJars, addedFiles}, ',');
    return allFiles.split(",");
  }

  private String[] getTempArchivesFromConf(Configuration conf) {
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDARCHIVES, addedArchives);
      return addedArchives.split(",");
    }
    return new String[0];
  }

  // TODO: add String[] skipJars
  /**
   * Localizes files, archives and jars from a provided array of names.
   * @param hdfsDirPathStr Destination directory in HDFS.
   * @param conf Configuration.
   * @param inputOutputJars The file names to localize.
   * @return List<LocalResource> local resources to add to execution
   * @throws IOException when hdfs operation fails.
   * @throws LoginException when getDefaultDestDir fails with the same exception
   */
  public List<LocalResource> localizeTempFiles(Path hdfsDirPathStr, Configuration conf,
      String[] inputOutputJars) throws IOException, LoginException {
    List<LocalResource> tmpResources = new ArrayList<LocalResource>();
    addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.FILE, inputOutputJars);
    return tmpResources;
  }

  private void addTempResources(Configuration conf,
      List<LocalResource> tmpResources, Path hdfsDirPathStr,
      LocalResourceType type,
      String[] files) throws IOException {
    if (files == null) return;
    for (String file : files) {
      if (!StringUtils.isNotBlank(file)) {
        continue;
      }
      Path hdfsFilePath = new Path(hdfsDirPathStr, getResourceBaseName(new Path(file)));
      LocalResource localResource = localizeResource(new Path(file),
          hdfsFilePath, type, conf);
      tmpResources.add(localResource);
    }
  }

  @SuppressWarnings("deprecation")
  public static FileStatus validateTargetDir(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(path);
    } catch (FileNotFoundException fe) {
      // do nothing
    }
    return (fstatus != null && fstatus.isDir()) ? fstatus : null;
  }

  // the api that finds the jar being used by this class on disk
  public String getExecJarPathLocal () throws URISyntaxException {
    // returns the location on disc of the jar of this class.
    return DAGUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
  }

  /*
   * Helper function to retrieve the basename of a local resource
   */
  public String getBaseName(LocalResource lr) {
    return FilenameUtils.getName(lr.getResource().getFile());
  }

  /**
   * @param path - the string from which we try to determine the resource base name
   * @return the name of the resource from a given path string.
   */
  public String getResourceBaseName(Path path) {
    return path.getName();
  }

  /**
   * @param src the source file.
   * @param dest the destination file.
   * @param conf the configuration
   * @return true if the file names match else returns false.
   * @throws IOException when any file system related call fails
   */
  private boolean checkPreExisting(FileSystem sourceFS, Path src, Path dest, Configuration conf)
      throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileStatus destStatus = FileUtils.getFileStatusOrNull(destFS, dest);
    if (destStatus != null) {
      return (sourceFS.getFileStatus(src).getLen() == destStatus.getLen());
    }
    return false;
  }

  /**
   * Localizes a resources. Should be thread-safe.
   * @param src path to the source for the resource
   * @param dest path in hdfs for the resource
   * @param type local resource type (File/Archive)
   * @param conf
   * @return localresource from mr3 localization.
   * @throws IOException when any file system related calls fails.
   */
  public LocalResource localizeResource(Path src, Path dest, LocalResourceType type, Configuration conf)
    throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileSystem srcFs;
    if (src.toUri().getScheme() != null) {
      srcFs = src.getFileSystem(conf);
    } else {
      srcFs = FileSystem.getLocal(conf);
    }

    if (!checkPreExisting(srcFs, src, dest, conf)) {
      // copy the src to the destination and create local resource.
      // do not overwrite.
      String srcStr = src.toString();
      LOG.info("Localizing resource because it does not exist: " + srcStr + " to dest: " + dest);
      Object notifierNew = new Object(),
          notifierOld = copyNotifiers.putIfAbsent(srcStr, notifierNew),
          notifier = (notifierOld == null) ? notifierNew : notifierOld;
      // To avoid timing issues with notifications (and given that HDFS check is anyway the
      // authoritative one), don't wait infinitely for the notifier, just wait a little bit
      // and check HDFS before and after.
      if (notifierOld != null
          && checkOrWaitForTheFile(srcFs, src, dest, conf, notifierOld, 1, 150, false)) {
        return createLocalResource(destFS, dest, type, LocalResourceVisibility.PRIVATE);
      }
      try {
        // FileUtil.copy takes care of copy from local filesystem internally.
        FileUtil.copy(srcFs, src, destFS, dest, false, false, conf);
        synchronized (notifier) {
          notifier.notifyAll(); // Notify if we have successfully copied the file.
        }
        copyNotifiers.remove(srcStr, notifier);
      } catch (IOException e) {
        if ("Exception while contacting value generator".equals(e.getMessage())) {
          // HADOOP-13155, fixed version: 2.8.0, 3.0.0-alpha1
          throw new IOException("copyFromLocalFile failed due to HDFS KMS failure", e);
        }

        LOG.info("Looks like another thread or process is writing the same file");
        int waitAttempts = HiveConf.getIntVar(
            conf, ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS);
        long sleepInterval = HiveConf.getTimeVar(
            conf, HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL, TimeUnit.MILLISECONDS);
        // Only log on the first wait, and check after wait on the last iteration.
        if (!checkOrWaitForTheFile(
            srcFs, src, dest, conf, notifierOld, waitAttempts, sleepInterval, true)) {
          LOG.error("Could not find the jar that was being uploaded");
          throw new IOException("Previous writer likely failed to write " + dest +
              ". Failing because I am unlikely to write too.");
        }
      } finally {
        if (notifier == notifierNew) {
          copyNotifiers.remove(srcStr, notifierNew);
        }
      }
    }
    return createLocalResource(destFS, dest, type, LocalResourceVisibility.PRIVATE);
  }

  public boolean checkOrWaitForTheFile(FileSystem srcFs, Path src, Path dest, Configuration conf,
      Object notifier, int waitAttempts, long sleepInterval, boolean doLog) throws IOException {
    for (int i = 0; i < waitAttempts; i++) {
      if (checkPreExisting(srcFs, src, dest, conf)) return true;
      if (doLog && i == 0) {
        LOG.info("Waiting for the file " + dest + " (" + waitAttempts + " attempts, with "
            + sleepInterval + "ms interval)");
      }
      try {
        if (notifier != null) {
          // The writing thread has given us an object to wait on.
          synchronized (notifier) {
            notifier.wait(sleepInterval);
          }
        } else {
          // Some other process is probably writing the file. Just sleep.
          Thread.sleep(sleepInterval);
        }
      } catch (InterruptedException interruptedException) {
        throw new IOException(interruptedException);
      }
    }
    return checkPreExisting(srcFs, src, dest, conf); // One last check.
  }

  /**
   * Creates and initializes a JobConf object that can be used to execute
   * the DAG. The configuration object will contain configurations from mapred-site
   * overlaid with key/value pairs from the hiveConf object. Finally it will also
   * contain some hive specific configurations that do not change from DAG to DAG.
   *
   * @param hiveConf Current hiveConf for the execution
   * @return JobConf base configuration for job execution
   */
  public JobConf createConfiguration(HiveConf hiveConf) {
    hiveConf.setBoolean("mapred.mapper.new-api", false);
    // mapred.mapper.new-api can be overridden in vertexJobConf created in initializeVertexConf()
    // e.g., for MapReduceMapWork created by MR3DistCp

    JobConf conf =
        TezConfigurationFactory
            .wrapWithJobConf(hiveConf, null);

    conf.set("mapred.output.committer.class", NullOutputCommitter.class.getName());

    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", false);
    conf.setBoolean("mapred.committer.job.task.cleanup.needed", false);

    conf.setClass("mapred.output.format.class", HiveOutputFormatImpl.class, OutputFormat.class);

    conf.set(MRJobConfig.OUTPUT_KEY_CLASS, HiveKey.class.getName());
    conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, BytesWritable.class.getName());

    conf.set("mapred.partitioner.class", HiveConf.getVar(conf, HiveConf.ConfVars.HIVEPARTITIONER));
    conf.set("tez.runtime.partitioner.class", MRPartitioner.class.getName());

    // Removing job credential entry/ cannot be set on the tasks
    conf.unset("mapreduce.job.credentials.binary");

    hiveConf.stripHiddenConfigurations(conf);

    // Remove hive configs which are used only in HS2 and not needed for execution
    conf.unset(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST.varname);

    String[] configRemoveKeys = HiveConf.getTrimmedStringsVar(hiveConf, ConfVars.HIVE_MR3_CONFIG_REMOVE_KEYS);
    for (String key : configRemoveKeys) {
      conf.unset(key);
    }

    String[] configRemovePrefixes = HiveConf.getTrimmedStringsVar(hiveConf, ConfVars.HIVE_MR3_CONFIG_REMOVE_PREFIXES);
    List<String> keysToRemove = new ArrayList<>();
    for (Map.Entry<String, String> entry : conf) {
      for (String name : configRemovePrefixes) {
        if (entry.getKey().startsWith(name)) {
          keysToRemove.add(entry.getKey());
        }
      }
    }
    for (String key : keysToRemove) {
      conf.unset(key);
    }

    return conf;
  }

  /**
   * Creates the mr3 Scratch dir for MR3Tasks
   */
  public Path createMr3ScratchDir(Path scratchDir, Configuration conf, boolean createDir)
      throws IOException {
    UserGroupInformation ugi;
    String userName;
    try {
      ugi = Utils.getUGI();
      userName = ugi.getShortUserName();
    } catch (LoginException e) {
      throw new IOException(e);
    }

    // Cf. HIVE-21171
    // ConfVars.HIVE_RPC_QUERY_PLAN == true, so we do not need mr3ScratchDir to store DAG Plans.
    // However, we may still need mr3ScratchDir if TezWork.configureJobConfAndExtractJars() returns
    // a non-empty list in MR3Task.
    Path mr3ScratchDir = getMr3ScratchDir(new Path(scratchDir, userName));
    LOG.info("mr3ScratchDir path " + mr3ScratchDir + " for user " + userName);
    if (createDir) {
      FileSystem fs = mr3ScratchDir.getFileSystem(conf);
      fs.mkdirs(mr3ScratchDir, new FsPermission(SessionState.TASK_SCRATCH_DIR_PERMISSION));
    }

    return mr3ScratchDir;
  }

  /**
   * Gets the mr3 Scratch dir for MR3Tasks
   */
  private Path getMr3ScratchDir(Path scratchDir) {
    return new Path(scratchDir, MR3_DIR + "-" + MR3SessionManagerImpl.getInstance().getUniqueId() + "-" + TaskRunner.getTaskRunnerID());
  }

  public void cleanMr3Dir( Path scratchDir, Configuration conf ) {
    try {
      FileSystem fs = scratchDir.getFileSystem(conf);
      fs.delete(scratchDir, true);
    } catch (Exception ex) {
      // This is a non-fatal error. Warn user they may need to clean up dir.
      LOG.warn("Error occurred while cleaning up MR3 scratch Dir: " + scratchDir, ex);
    }
  }

  private void setupAutoReducerParallelism(TezEdgeProperty edgeProp, Vertex v, JobConf jobConf)
    throws IOException {
    if (edgeProp.isAutoReduce()) {
      String vertexManagerClassName = ShuffleVertexManager.class.getName();

      Configuration pluginConf = new Configuration(false);
      pluginConf.setBoolean(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, true);
      pluginConf.setInt(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
          edgeProp.getMinReducer());
      pluginConf.setLong(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          edgeProp.getInputSizePerReducer());
      // For vertices on which Hive enables auto parallelism, we should ignore the following two parameters.
      pluginConf.setInt(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MIN_NUM_TASKS, 1);
      pluginConf.setInt(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MAX_REDUCTION_PERCENTAGE, 0);

      // Cf. Hive uses default values for minSrcFraction and maxSrcFraction.
      // However, ShuffleVertexManagerBase.getComputeRoutingAction() uses config.getMaxFraction().
      setupMinMaxSrcFraction(jobConf, pluginConf);

      // TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL == true, so load configs for using stats
      setupAutoParallelismUsingStats(jobConf, pluginConf);

      ByteString userPayload = TezUtils.createByteStringFromConf(pluginConf);
      EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(
          vertexManagerClassName, userPayload);

      v.setVertexManagerPlugin(vertexManagerPluginDescriptor);
      LOG.info("Set VertexManager: ShuffleVertexManager(AUTO_PARALLEL) {} {}", v.getName(), true);
    }
  }

  public void setupMinMaxSrcFraction(JobConf jobConf, Configuration pluginConf) {
    float minSrcFraction = jobConf.getFloat(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    pluginConf.setFloat(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, minSrcFraction);

    float maxSrcFraction = jobConf.getFloat(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);
    pluginConf.setFloat(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, maxSrcFraction);
  }

  private void setupAutoParallelismUsingStats(JobConf jobConf, Configuration pluginConf) {
    boolean useStatsAutoParallelism = jobConf.getBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_USE_STATS_AUTO_PARALLELISM,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_USE_STATS_AUTO_PARALLELISM_DEFAULT);
    int autoParallelismMinPercent = jobConf.getInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLELISM_MIN_PERCENT,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLELISM_MIN_PERCENT_DEFAULT);
    pluginConf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_USE_STATS_AUTO_PARALLELISM,
        useStatsAutoParallelism);
    pluginConf.setInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLELISM_MIN_PERCENT,
        autoParallelismMinPercent);
  }

  private void setupQuickStart(TezEdgeProperty edgeProp, Vertex v, JobConf jobConf)
    throws IOException {
    if (!edgeProp.isSlowStart()) {
      String vertexManagerClassName = ShuffleVertexManager.class.getName();

      boolean isAutoParallelism = edgeProp.isFixed() ? false :
          jobConf.getBoolean(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
              ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT);
      // TODO: check 'assert isAutoParallelism == false'
      Configuration pluginConf;
      if (isAutoParallelism) {
        pluginConf = createPluginConfShuffleVertexManagerAutoParallel(jobConf);
      } else {
        pluginConf = createPluginConfShuffleVertexManagerFixed(jobConf);
      }
      pluginConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 0);
      pluginConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 0);

      ByteString userPayload = TezUtils.createByteStringFromConf(pluginConf);
      EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(
          vertexManagerClassName, userPayload);

      v.setVertexManagerPlugin(vertexManagerPluginDescriptor);
      LOG.info("Set VertexManager: ShuffleVertexManager(QuickStart) {} {}", v.getName(), isAutoParallelism);
    }
  }

  public Configuration createPluginConfShuffleVertexManagerAutoParallel(JobConf jobConf) {
    Configuration pluginConf = new Configuration(false);

    pluginConf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, true);

    int minTaskParallelism = jobConf.getInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT);
    pluginConf.setInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM, minTaskParallelism);

    long desiredTaskInputSize = jobConf.getLong(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    pluginConf.setLong(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, desiredTaskInputSize);

    int autoParallelismMinNumTasks = jobConf.getInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MIN_NUM_TASKS,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MIN_NUM_TASKS_DEFAULT);
    pluginConf.setInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MIN_NUM_TASKS,
        autoParallelismMinNumTasks);

    int autoParallelismMaxReductionPercentage = jobConf.getInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MAX_REDUCTION_PERCENTAGE,
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MAX_REDUCTION_PERCENTAGE_DEFAULT);
    pluginConf.setInt(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_AUTO_PARALLEL_MAX_REDUCTION_PERCENTAGE,
        autoParallelismMaxReductionPercentage);

    setupAutoParallelismUsingStats(jobConf, pluginConf);

    return pluginConf;
  }

  public Configuration createPluginConfShuffleVertexManagerFixed(JobConf jobConf) {
    Configuration pluginConf = new Configuration(false);

    pluginConf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, false);

    return pluginConf;
  }

  /**
   * MR3 Requires Vertices to have VertexManagers. The Current TezWork to Hive-MR3 Dag can create
   * Vertices without VertexManagers. This method is used to post-process the Hive-MR3 Dag to
   * get the correct VertexManager for the given Vertex parameter.
   * originally from VertexImpl.java of Tez
   *
   * @param vertex
   * @return EntityDescriptor that contains the Vetex's VertexManager
   * @throws IOException
   */
  public EntityDescriptor getVertexManagerForVertex(
      Vertex vertex,
      ByteString userPayloadRootInputVertexManager,
      ByteString userPayloadShuffleVertexManagerAuto,
      ByteString userPayloadShuffleVertexManagerFixed) {
    assert vertex.getVertexManagerPlugin() == null;

    boolean hasBipartite = false;
    boolean hasOneToOne = false;
    boolean hasCustom = false;
    boolean hasFixed = false;
    for (Edge edge : vertex.getInputEdges()) {
      switch (edge.getEdgeProperty().getDataMovementType()) {
        case SCATTER_GATHER:
          hasBipartite = true;
          break;
        case ONE_TO_ONE:
          hasOneToOne = true;
          break;
        case BROADCAST:
          break;
        case CUSTOM:
          hasCustom = true;
          break;
        default:
          throw new MR3UncheckedException("Unknown data movement type: " +
                  edge.getEdgeProperty().getDataMovementType());
      }
      if (edge.getEdgeProperty().isFixed()) {
        LOG.info("Set VertexManager: Edge from {} to {} is {}, FIXED",
            edge.getSrcVertex().getName(), edge.getDestVertex().getName(), edge.getEdgeProperty().getDataMovementType());
        hasFixed = true;
      }
    }

    boolean hasInputInitializers = false;

    for (Map.Entry<String, DataSource> dsEntry : vertex.getDataSources().entrySet()) {
      if (dsEntry.getValue().hasInputInitializer()) {
        hasInputInitializers = true;
        break;
      }
    }

    // Intended order of picking a vertex manager
    // If there is an InputInitializer then we use the RootInputVertexManager. May be fixed by TEZ-703
    // If there is a custom edge we fall back to default ImmediateStartVertexManager
    // If there is a one to one edge then we use the InputReadyVertexManager
    // If there is a scatter-gather edge then we use the ShuffleVertexManager
    // Else we use the default ImmediateStartVertexManager
    EntityDescriptor vertexManagerPluginDescriptor = null;
    String rootInputVertexManagerClassName =
            "org.apache.tez.dag.app.dag.impl.RootInputVertexManager";
    String immediateStartVertexManagerClassName =
            "org.apache.tez.dag.app.dag.impl.ImmediateStartVertexManager";

    if (hasInputInitializers) {
      vertexManagerPluginDescriptor = new EntityDescriptor(
              rootInputVertexManagerClassName, userPayloadRootInputVertexManager);
      LOG.info("Set VertexManager: RootInputVertexManager {}", vertex.getName());
    } else if (hasOneToOne && !hasCustom) {
      vertexManagerPluginDescriptor = new EntityDescriptor(
              InputReadyVertexManager.class.getName(), null);
      LOG.info("Set VertexManager: InputReadyVertexManager {}", vertex.getName());
    } else if (hasBipartite && !hasCustom) {
      ByteString userPayloadShuffleVertexManager =
          hasFixed ? userPayloadShuffleVertexManagerFixed : userPayloadShuffleVertexManagerAuto;
      vertexManagerPluginDescriptor = new EntityDescriptor(
              ShuffleVertexManager.class.getName(), userPayloadShuffleVertexManager);
      LOG.info("Set VertexManager: ShuffleVertexManager(Missing): {} {}", vertex.getName(), !hasFixed);
    } else {
      //schedule all tasks upon vertex start. Default behavior.
      vertexManagerPluginDescriptor = new EntityDescriptor(
              immediateStartVertexManagerClassName, null);
      LOG.info("Set VertexManager: ImmediateStartVertexManager {}", vertex.getName());
    }

    return vertexManagerPluginDescriptor;
  }
}
