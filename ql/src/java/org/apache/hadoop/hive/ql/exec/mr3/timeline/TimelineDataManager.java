/**
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

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.datamonad.mr3.history.EntityKey;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.security.ACLManager;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.security.ACLType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.slf4j.LoggerFactory;

/**
 * The class wrap over the timeline store and the ACLs manager. It does some non
 * trivial manipulation of the timeline data before putting or after getting it
 * from the timeline store, and checks the user's access to it.
 * 
 */
public class TimelineDataManager {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TimelineDataManager.class);

  public static final String DEFAULT_DOMAIN_ID = "DEFAULT";

  private static TimelineDataManager instance;

  public static TimelineDataManager getInstance() {
    return instance;
  }

  public static TimelineDataManager createInstance(TimelineStore store, ACLManager aclManager) {
    instance = new TimelineDataManager(store, aclManager);
    return instance;
  }

  private final TimelineStore store;
  private final ACLManager aclManager;

  private TimelineDataManager(TimelineStore store, ACLManager aclManager) {
    this.store = store;
    this.aclManager = aclManager;
  }

  public void initialize() throws Exception {
    TimelineDomain domain = store.getDomain(DEFAULT_DOMAIN_ID);
    // it is okay to reuse an existing domain even if it was created by another
    // user of the timeline server before, because it allows everybody to access.
    if (domain == null) {
      // create a default domain, which allows everybody to access and modify
      // the entities in it.
      domain = new TimelineDomain();
      domain.setId(DEFAULT_DOMAIN_ID);
      domain.setDescription("Default Domain");
      domain.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      domain.setReaders("*");
      domain.setWriters("*");
      store.put(domain);
    }

    if (aclManager != null && aclManager.isAclsEnabled()) {
      domain = store.getDomain(DEFAULT_DOMAIN_ID);
      domain.setWriters("MR3_APP_MASTER");
      store.put(domain);
    }
  }

  public interface CheckAcl {
    boolean check(TimelineEntity entity) throws IOException;
  }

  class CheckAclImpl implements CheckAcl {
    final UserGroupInformation ugi;

    public CheckAclImpl(UserGroupInformation callerUGI) {
      ugi = callerUGI;
    }

    public boolean check(TimelineEntity entity) throws IOException {
      Object viewer = entity.getOtherInfo().get(EntityKey.aclViewer());
      if (viewer == null || ACLManager.WILDCARD_ACL_VALUE.equals(viewer)) {
        return true;
      }
      String user = ugi.getShortUserName();
      if (user.equals(viewer)) {
        return true;
      }
      return aclManager != null && aclManager.checkAccess(ugi, ACLType.AM_VIEW_ACL);
    }
  }

  /**
   * Get the timeline entities that the given user have access to. The meaning
   * of each argument has been documented with
   * {@link TimelineReader#getEntities}.
   * 
   * @see TimelineReader#getEntities
   */
  public TimelineEntities getEntities(
      String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilter,
      Long windowStart,
      Long windowEnd,
      String fromId,
      Long fromTs,
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws IOException {
    TimelineEntities entities = null;
    entities = store.getEntities(
        entityType,
        limit,
        windowStart,
        windowEnd,
        fromId,
        fromTs,
        primaryFilter,
        secondaryFilter,
        fields,
        new CheckAclImpl(callerUGI));

    if (entities == null) {
      return new TimelineEntities();
    }
    return entities;
  }

  /**
   * Get the single timeline entity that the given user has access to. The
   * meaning of each argument has been documented with
   * {@link TimelineReader#getEntity}.
   * 
   * @see TimelineReader#getEntity
   */
  public TimelineEntity getEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws IOException {
    TimelineEntity entity = null;
    entity = store.getEntity(entityId, entityType, fields);
    if (entity != null) {
      CheckAcl checkAcl = new CheckAclImpl(callerUGI);
      if (!checkAcl.check(entity)) {
        entity = null;
      }
    }
    return entity;
  }

  /**
   * Get the events whose entities the given user has access to. The meaning of
   * each argument has been documented with
   * {@link TimelineReader#getEntityTimelines}.
   * 
   * @see TimelineReader#getEntityTimelines
   */
  public TimelineEvents getEvents(
      String entityType,
      SortedSet<String> entityIds,
      SortedSet<String> eventTypes,
      Long windowStart,
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws IOException {
    TimelineEvents events = null;
    events = store.getEntityTimelines(
        entityType,
        entityIds,
        limit,
        windowStart,
        windowEnd,
        eventTypes);
    if (events != null) {
      Iterator<TimelineEvents.EventsOfOneEntity> eventsItr =
          events.getAllEvents().iterator();
      CheckAcl checkAcl = new CheckAclImpl(callerUGI);
      while (eventsItr.hasNext()) {
        TimelineEvents.EventsOfOneEntity eventsOfOneEntity = eventsItr.next();
        try {
          TimelineEntity entity = store.getEntity(
              eventsOfOneEntity.getEntityId(),
              eventsOfOneEntity.getEntityType(),
              EnumSet.of(Field.PRIMARY_FILTERS));
          if (!checkAcl.check(entity)) {
            eventsItr.remove();
          }
        } catch (Exception e) {
          LOG.error("Error when verifying access for user " + callerUGI
              + " on the events of the timeline entity "
              + new EntityIdentifier(eventsOfOneEntity.getEntityId(),
                  eventsOfOneEntity.getEntityType()), e);
          eventsItr.remove();
        }
      }
    }
    if (events == null) {
      return new TimelineEvents();
    }
    return events;
  }

  /**
   * Store the timeline entities into the store and set the owner of them to the
   * given user.
   */
  public TimelinePutResponse postEntity(
      TimelineEntity entity) throws IOException {
    TimelineEntities entitiesToPut = new TimelineEntities();

    // if the domain id is not specified, the entity will be put into the default domain
    assert entity.getDomainId() == null || entity.getDomainId().isEmpty();
    entity.setDomainId(DEFAULT_DOMAIN_ID);

    // postEntity() is called by HistoryLogger of DAGAppMaster, so do not call checkAccess()
    entitiesToPut.addEntity(entity);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing the entity " + entity.getEntityId() + ", JSON-style content: "
          + TimelineUtils.dumpTimelineRecordtoJSON(entity));
    }
    TimelinePutResponse response = store.put(entitiesToPut);
    return response;
  }

  public TimelinePutResponse postEntities(
      List<TimelineEntity> entities) throws IOException {
    TimelineEntities entitiesToPut = new TimelineEntities();

    for (TimelineEntity entity: entities) {
      assert entity.getDomainId() == null || entity.getDomainId().isEmpty();
      entity.setDomainId(DEFAULT_DOMAIN_ID);
    }

    entitiesToPut.addEntities(entities);
    if (LOG.isDebugEnabled()) {
      for (TimelineEntity entity: entities) {
        LOG.debug("Storing entity " + entity.getEntityId() + ", JSON-style content: "
          + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
    }
    TimelinePutResponse response = store.put(entitiesToPut);
    return response;
  }
}
