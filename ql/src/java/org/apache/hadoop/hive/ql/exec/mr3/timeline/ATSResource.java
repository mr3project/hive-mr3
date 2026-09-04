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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import com.datamonad.mr3.history.EntityKey;
import com.datamonad.mr3.history.EntityType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.slf4j.LoggerFactory;

/**
 * ATS-style endpoints: GET /ats/timeline/{entityType}[/{entityId}]
 */
// prefix "/ats" should be omitted because it is the mount path.
@Path("/timeline")
public class ATSResource {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ATSResource.class);

  private final TimelineDataManager dataManager;

  public ATSResource() {
    this.dataManager = TimelineDataManager.getInstance();
    // instantiated per incoming request (the default per-request lifecycle in JAX-RS) by Jersey
    if (LOG.isDebugEnabled()) {
      LOG.debug("ATSResource initialized with TimelineDataManager");
    }
  }

  // type = EntityType.MR3_DAG, EntityType.MR3_APP_ATTEMPT
  // returns TimelineEntity[] in types/index.ts
  @GET @Path("{type}") @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntities list(
      @PathParam("type") String type,
      @QueryParam("primaryFilter") String primary,
      @QueryParam("secondaryFilter") List<String> secondary,
      @QueryParam("limit") Long limit,
      @QueryParam("windowStart") Long windowStart,
      @QueryParam("windowEnd") Long windowEnd,
      @QueryParam("fromId") String fromId,
      @QueryParam("fromTs") Long fromTs,
      @QueryParam("fields") String fields,
      @Context HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      remoteUser = "anonymous";
    }
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(remoteUser);

    NameValuePair p0 = parsePair(primary);
    Collection<NameValuePair> secs = parsePairs(secondary);
    EnumSet<TimelineReader.Field> fs = parseFields(fields);

    LOG.info("ATSResource.list() from {}: type={} limit={}", remoteUser, type, limit);
    Long newLimit;
    if (limit == null && (type.equals("MR3_DAG_SUMMARY") || type.equals("MR3_CONTAINER"))) {
      // avoid using the default value TimelineReader.DEFAULT_LIMIT in getEntities()
      newLimit = Long.MAX_VALUE;
    } else {
      newLimit = limit;
    }

    try {
      TimelineEntities timelineEntities = dataManager.getEntities(
          type, p0, secs, windowStart, windowEnd, fromId, fromTs, newLimit, fs, ugi);
      return setCurrentTimeAndtrimTimelineEntities(type, timelineEntities);
    } catch (IOException e) {
       throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private TimelineEntities setCurrentTimeAndtrimTimelineEntities(
      String type, TimelineEntities timelineDags) throws IOException {
    final TimelineEntities results = new TimelineEntities();
    final long currentTime = System.currentTimeMillis();
    boolean isMr3Dag = type.equals(EntityType.MR3_DAG());

    for (TimelineEntity current: timelineDags.getEntities()) {
      // EntityKey.currentTime is set only here and not in HistoryEvent
      current.addOtherInfo(EntityKey.currentTime(), java.lang.Long.valueOf(currentTime));
      if (isMr3Dag) {
        // remove dagProto to reduce the size
        current.addOtherInfo(EntityKey.dagProto(), null);
      }
      results.addEntity(current);
    }

    return results;
  }

  // Detail entity
  // type = EntityType.MR3_DAG, EntityType.MR3_VERTEX
  // returns TimelineEntity in types/index.ts
  @GET @Path("{type}/{id}") @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity detail(
      @PathParam("type") String type,
      @PathParam("id")   String id,
      @QueryParam("fields") String fields,
      @Context HttpServletRequest request) {
    // Obtain UGI similarly if needed for ACL checks
    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      remoteUser = "anonymous";
    }
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(remoteUser);

    LOG.info("ATSResource.detail() from {}: type={} id={}", remoteUser, type, id);

    EnumSet<TimelineReader.Field> fs = parseFields(fields);
    try {
      TimelineEntity e = dataManager.getEntity(type, id, fs, ugi);
      if (e == null) throw new WebApplicationException(Response.Status.NOT_FOUND);
      final long currentTime = System.currentTimeMillis();
      e.addOtherInfo(EntityKey.currentTime(), java.lang.Long.valueOf(currentTime));
      return e;
    } catch (IOException e) {
      throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  // Helpers
  private static NameValuePair parsePair(String s) {
    if (s == null) return null;
    String[] a = s.split(":",2);
    return new NameValuePair(a[0], a[1]);
  }

  private static Collection<NameValuePair> parsePairs(List<String> list) {
    if (list == null) return null;
    List<NameValuePair> out = new ArrayList<>();
    for (String s: list) out.add(parsePair(s));
    return out;
  }

  private static EnumSet<TimelineReader.Field> parseFields(String s) {
    if (s == null) return null;
    String[] toks = s.split(",");
    EnumSet<TimelineReader.Field> fs = EnumSet.noneOf(TimelineReader.Field.class);
    for (String t: toks) fs.add(TimelineReader.Field.valueOf(t.trim().toUpperCase()));
    return fs;
  }
}
