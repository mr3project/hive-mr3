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
import com.datamonad.mr3.api.client.Progress;
import com.datamonad.mr3.api.client.VertexStatus;
import com.datamonad.mr3.common.Utils;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import scala.collection.JavaConverters;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * AM-proxy endpoints: GET /proxy/mr3/{path}[/{id}]
 */
// prefix "/proxy" should be omitted because it is the mount path.
@Path("/mr3")
public class AMProxyResource {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AMProxyResource.class);

  private final MR3LiveStatusService liveStatusService;
  private final ObjectMapper mapper = new ObjectMapper();

  public AMProxyResource() {
    this.liveStatusService = new MR3LiveStatusService();
    // instantiated per incoming request (the default per-request lifecycle in JAX-RS) by Jersey
    if (LOG.isDebugEnabled()) {
      LOG.debug("AMProxyResource initialized with MR3LiveStatusService");
    }
  }

  // AppAttempt

  // do not check permission with ACLManager
  @GET @Path("currentAppAttemptId") @Produces(MediaType.APPLICATION_JSON)
  public Response getCurrentAppAttemptId() {
    LOG.info("AMProxyResource.getCurrentAppAttemptId()");

    String currentAppAttemptId = liveStatusService.getApplicationAttemptId();
    ObjectNode root = mapper.createObjectNode();
    root.put("currentAppAttemptId", currentAppAttemptId);

    ObjectNode wrapper = mapper.createObjectNode();
    wrapper.set("appAttemptId", root);
    return jsonResponse(wrapper);
  }

  // do not check permission with ACLManager
  // TODO: "workers=*" is currently unused.
  @GET @Path("appAttemptInfo") @Produces(MediaType.APPLICATION_JSON)
  public Response getAppAttemptInfo(
      @QueryParam("appAttemptId") String appAttemptId,
      @QueryParam("workers") String workers) {
    checkAppAttemptId(appAttemptId);

    LOG.info("AMProxyResource.getAppAttemptInfo(): appAttemptId={}", appAttemptId);

    AppAttemptStatus appAttemptStatus = liveStatusService.getAppAttemptStatus();

    ObjectNode root = mapper.createObjectNode();
    root.put("appAttemptStatus", convertAppAttemptStatus(mapper, appAttemptStatus));

    ObjectNode wrapper = mapper.createObjectNode();
    wrapper.set("appAttempt", root);
    return jsonResponse(wrapper);
  }

  // DAG

  @GET @Path("dagInfo") @Produces(MediaType.APPLICATION_JSON)
  public Response getDagInfo(
      @QueryParam("appAttemptId") String appAttemptId,
      @QueryParam("dagIdId") String dagIdIdStr,
      @QueryParam("counters") String counters,
      @Context HttpServletRequest request) {
    checkAppAttemptId(appAttemptId);

    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      remoteUser = "anonymous";
    }

    final int dagIdId = Integer.parseInt(dagIdIdStr);
    final boolean allCounters = "*".equals(counters);
    LOG.info("AMProxyResource.getDagInfo() from {}: dagIdId={}", remoteUser, dagIdId);

    DAGStatus dagStatus = liveStatusService.getDagStatus(dagIdId, allCounters, remoteUser);

    ObjectNode root = mapper.createObjectNode();
    root.put("dagStatus", convertDAGStatus(mapper, dagStatus, allCounters));

    ObjectNode wrapper = mapper.createObjectNode();
    wrapper.set("dag", root);
    return jsonResponse(wrapper);
  }

  // Vertex

  @GET @Path("vertexInfo")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getVertexInfo(
      @QueryParam("appAttemptId") String appAttemptId,
      @QueryParam("dagIdId") String dagIdIdStr,
      @QueryParam("vertexName") String vertexName,
      @QueryParam("counters") String counters,
      @Context HttpServletRequest request) {
    checkAppAttemptId(appAttemptId);

    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      remoteUser = "anonymous";
    }

    int dagIdId = Integer.parseInt(dagIdIdStr);
    final boolean allCounters = "*".equals(counters);
    LOG.info("AMProxyResource.getVertexInfo() from {}: dagIdId={}, vertexName={}", remoteUser, dagIdId, vertexName);

    VertexStatus vertexStatus = liveStatusService.getVertexStatus(dagIdId, vertexName, true, remoteUser);

    ObjectNode root = mapper.createObjectNode();
    root.put("vertexStatus", convertVertexStatus(mapper, vertexStatus, allCounters));

    ObjectNode wrapper = mapper.createObjectNode();
    wrapper.set("vertex", root);
    return jsonResponse(wrapper);
  }

  //
  // common methods
  //

  private void checkAppAttemptId(String appAttemptId) {
    String currentAppAttemptId = liveStatusService.getApplicationAttemptId();
    if (!currentAppAttemptId.equals(appAttemptId)) {
      LOG.warn("Invalid appAttemptId: {}", appAttemptId);

      ObjectNode err = mapper.createObjectNode()
        .put("error", "Invalid appAttemptId")
        .put("expected", currentAppAttemptId)
        .put("got", appAttemptId);

      throw new WebApplicationException(
        Response.status(Response.Status.NOT_FOUND)
          .entity(err.toString())
          .type(MediaType.APPLICATION_JSON)
          .build());
    }
  }

  private static Response jsonResponse(ObjectNode node) {
    return Response.ok(node.toString(), MediaType.APPLICATION_JSON_TYPE).build();
  }

  private static ObjectNode convertAppAttemptStatus(
      ObjectMapper mapper, AppAttemptStatus appAttemptStatus) {
    if (appAttemptStatus == null) {
      return null;
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("numSucceededDags", appAttemptStatus.numSucceededDags());
    node.put("numFailedDags", appAttemptStatus.numFailedDags());
    node.put("numKilledDags", appAttemptStatus.numKilledDags());
    node.put("numPendingDags", appAttemptStatus.numPendingDags());

    ObjectNode runningDagQueueNode = mapper.createObjectNode();
    Map<String, String> runningDagQueueMap = JavaConverters.mapAsJavaMapConverter(appAttemptStatus.runningDagQueueMap()).asJava();
    if (runningDagQueueMap != null) {
      for (Map.Entry<String, String> entry : runningDagQueueMap.entrySet()) {
        String dagId = entry.getKey();
        String queue = entry.getValue();
        runningDagQueueNode.put(dagId, queue);
      }
    }
    node.set("runningDagQueueMap", runningDagQueueNode);

    node.put("dagQueueScheme", appAttemptStatus.dagQueueScheme());
    node.put("dagQueueCapacitySpecs", appAttemptStatus.dagQueueCapacitySpecs());

    node.put("numRunningContainers", appAttemptStatus.numRunningContainers());

    Utils.MR3Resource totalContainerCapacity = appAttemptStatus.allAmContainerStatus().totalContainerCapacity();
    node.put("totalContainerCapacityMemoryInMb", totalContainerCapacity.memoryMb());
    node.put("totalContainerCapacityCores",
        totalContainerCapacity.cores() / totalContainerCapacity.divisor());

    node.put("numRunningWorkerTaskAttempts",
        appAttemptStatus.allAmContainerStatus().numRunningWorkerTaskAttempts());
    node.put("runningTaskAttemptsResourceMemoryInMb",
        appAttemptStatus.allAmContainerStatus().runningTaskAttemptsResource().memoryMb());

    ObjectNode dagResourceMapNode = mapper.createObjectNode();
    Map<String, Utils.MR3Resource> dagResourceMap = JavaConverters.mapAsJavaMapConverter(
        appAttemptStatus.allAmContainerStatus().dagResourceMap()).asJava();
    if (dagResourceMap != null) {
      for (Map.Entry<String, Utils.MR3Resource> entry : dagResourceMap.entrySet()) {
        String dagId = entry.getKey();
        long memoryMb = entry.getValue().memoryMb();
        dagResourceMapNode.put(dagId, memoryMb);
      }
    }
    node.set("dagResourceMap", dagResourceMapNode);

    return node;
  }

  /**
   * Converts a Scala Progress object to a JSON ObjectNode.
   * @param progress The Progress object.
   * @return An ObjectNode representing the Progress.
   */
  private static ObjectNode convertProgress(ObjectMapper mapper, Progress progress) {
    if (progress == null) {
      return null;
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("numTasks", progress.numTasks());
    node.put("numScheduledTasks", progress.numScheduledTasks());
    node.put("numRunningTasks", progress.numRunningTasks());
    node.put("numSucceededTasks", progress.numSucceededTasks());
    node.put("numFailedTasks", progress.numFailedTasks());
    node.put("numKilledTasks", progress.numKilledTasks());
    node.put("numFailedTaskAttempts", progress.numFailedTaskAttempts());
    node.put("numKilledTaskAttempts", progress.numKilledTaskAttempts());
    return node;
  }

  /**
   * Converts a Scala VertexStatus object to a JSON ObjectNode.
   * @param vertexStatus The VertexStatus object.
   * @return An ObjectNode representing the VertexStatus.
   */
  public static ObjectNode convertVertexStatus(
      ObjectMapper mapper, VertexStatus vertexStatus, boolean includeCounters) {
    if (vertexStatus == null) {
      return null;
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("state", vertexStatus.state().toString());
    node.put("vertexIdId", vertexStatus.vertexIdId());
    node.set("progress", convertProgress(mapper, vertexStatus.progress()));

    if (includeCounters) {
      addCounters(node, vertexStatus.counters());
    }

    node.put("priority", vertexStatus.priority());
    if (vertexStatus.startTime().isDefined()) {
      node.put("startTime", (long)vertexStatus.startTime().get());
    }
    if (vertexStatus.firstLaunchTime().isDefined()) {
      node.put("firstLaunchTime", (long)vertexStatus.firstLaunchTime().get());
    }
    if (vertexStatus.endTime().isDefined()) {
      node.put("endTime", (long)vertexStatus.endTime().get());
    }

    return node;
  }

  private static void addCounters(ObjectNode node, scala.Option<TezCounters> counters) {
    Map<String, Map<String, String>> countersMap = DAGStatus.countersToJavaMap(counters);
    if (!countersMap.isEmpty()) {
      ObjectNode countersNode = node.putObject("counters");
      for (Map.Entry<String, Map<String, String>> e : countersMap.entrySet()) {
        ObjectNode groupNode = countersNode.putObject(e.getKey());
        for (Map.Entry<String, String> p : e.getValue().entrySet()) {
          groupNode.put(p.getKey(), p.getValue());
        }
      }
    }
  }

  /**
   * Converts a Scala DAGStatus object to a JSON ObjectNode.
   * @param dagStatus The DAGStatus object.
   * @return An ObjectNode representing the DAGStatus.
   */
  public static ObjectNode convertDAGStatus(
      ObjectMapper mapper, DAGStatus dagStatus, boolean includeCounters) {
    if (dagStatus == null) {
      return null;
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("state", dagStatus.state().toString());
    node.set("progress", convertProgress(mapper, dagStatus.progress()));

    if (includeCounters) {
      addCounters(node, dagStatus.counters());
    }

    // Handle the vertexStatusMap
    ObjectNode vertexMapNode = node.putObject("vertexStatusMap");
    // Convert Scala Map to Java Map to iterate
    Map<String, VertexStatus> javaVertexMap = JavaConverters.mapAsJavaMapConverter(dagStatus.vertexStatusMap()).asJava();
    for (Map.Entry<String, VertexStatus> entry : javaVertexMap.entrySet()) {
      vertexMapNode.set(entry.getKey(), convertVertexStatus(mapper, entry.getValue(), includeCounters));
    }

    node.put("numSucceedTaskAttempts", dagStatus.numSucceedTaskAttempts());
    node.put("numFailedTaskAttempts", dagStatus.numFailedTaskAttempts());
    node.put("numKilledTaskAttempts", dagStatus.numKilledTaskAttempts());
    node.put("numLaunchedTaskAttempts", dagStatus.numLaunchedTaskAttempts());
    node.put("numHostLocalTaskAttempts", dagStatus.numHostLocalTaskAttempts());
    node.put("numHostNonLocalTaskAttempts", dagStatus.numHostNonLocalTaskAttempts());

    // Handle diagnostics
    ArrayNode diagnosticsNode = node.putArray("diagnostics");
    // Convert Scala List to Java List to iterate
    List<String> javaDiagnostics = JavaConverters.seqAsJavaListConverter(dagStatus.diagnostics()).asJava();
    for (String diag : javaDiagnostics) {
      diagnosticsNode.add(diag);
    }

    return node;
  }
}
