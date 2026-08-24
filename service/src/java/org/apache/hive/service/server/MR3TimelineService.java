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

package org.apache.hive.service.server;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.hive.ql.exec.mr3.timeline.AMProxyResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.ATSResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MR3LiveStatusService;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MR3TimelineIngestionService;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.LeveldbTimelineStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MemoryTimelineStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MR3LiveStatusServiceInterface;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MR3TimelineIngestionServiceInterface;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.ServerResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.TimelineDataManager;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.TimelineStore;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.http.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns MR3-UI resources and the active HiveServer2 timeline writer.
 *
 * <p>MR3-UI reuses the HiveServer2 WebUI connector and security settings.
 * The {@code mr3.ui.server.*} settings for a dedicated MR3 HTTP server are
 * therefore intentionally not used here.</p>
 */
final class MR3TimelineService {
  private static final Logger LOG = LoggerFactory.getLogger(MR3TimelineService.class);
  private static final String UI_INDEX = "hive-webapps/hiveserver2/index.html";

  private final HiveConf conf;
  private TimelineStore timelineStore;
  private volatile TimelineDataManager timelineDataManager;
  private MR3LiveStatusServiceInterface liveStatusService;
  private MR3TimelineIngestionServiceInterface ingestionService;
  private boolean enabled;
  private boolean active;

  MR3TimelineService(HiveConf conf) {
    this.conf = conf;
  }

  synchronized void initialize(HttpServer webServer) throws IOException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_CREATE_SERVER)) {
      LOG.info("MR3-UI is disabled by {}", HiveConf.ConfVars.HIVE_MR3_UI_CREATE_SERVER.varname);
      return;
    }

    validateStaticAssets();
    liveStatusService = MR3LiveStatusService.getInstance();
    // The embedded frontend must use these /mr3-api prefixes instead of the
    // /ats, /proxy, and /server paths used by the standalone MR3 HTTP server.
    webServer.addServlet("mr3_ats", "/mr3-api/ats/*",
        createJerseyServlet(ATSResource.class));
    webServer.addServlet("mr3_proxy", "/mr3-api/proxy/*",
        createJerseyServlet(AMProxyResource.class));
    webServer.addServlet("mr3_server", "/mr3-api/server/*",
        createJerseyServlet(ServerResource.class));
    enabled = true;
  }

  synchronized void activate() throws IOException {
    if (!enabled || active) {
      return;
    }

    try {
      timelineStore = createTimelineStore();
      timelineStore.initialize(conf);
      timelineDataManager = TimelineDataManager.createInstance(timelineStore, null);
      timelineDataManager.initialize();
      ingestionService = new MR3TimelineIngestionService();
      active = true;
      LOG.info("Activated MR3-UI on this HiveServer2 instance");
    } catch (Exception e) {
      deactivateAfterFailure();
      throw new IOException("Failed to start the MR3 timeline writer", e);
    }
  }

  synchronized void deactivate() {
    if (!active && timelineStore == null) {
      return;
    }

    if (ingestionService != null) {
      ingestionService.close();
      ingestionService = null;
    }
    timelineDataManager = null;
    closeTimelineStore();
    active = false;
    LOG.info("Deactivated MR3-UI on this HiveServer2 instance");
  }

  synchronized void stop() {
    deactivate();
    if (liveStatusService != null) {
      liveStatusService.close();
      liveStatusService = null;
    }
    enabled = false;
  }

  private TimelineStore createTimelineStore() {
    String storeType = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_TIMELINE_STORE_TYPE);
    if ("memory".equalsIgnoreCase(storeType)) {
      return new MemoryTimelineStore();
    }
    if ("leveldb".equalsIgnoreCase(storeType)) {
      return new LeveldbTimelineStore();
    }
    throw new IllegalArgumentException("Unsupported MR3 timeline store type: " + storeType);
  }

  private void validateStaticAssets() throws IOException {
    URL index = getClass().getClassLoader().getResource(UI_INDEX);
    if (index == null) {
      throw new IOException("MR3-UI static asset is missing: " + UI_INDEX);
    }
  }

  private ServletHolder createJerseyServlet(Class<?> resourceClass) {
    ResourceConfig config = new ResourceConfig()
        .register(resourceClass)
        .register(JacksonFeature.class);
    return new ServletHolder(new ServletContainer(config));
  }

  private void deactivateAfterFailure() {
    timelineDataManager = null;
    closeTimelineStore();
    active = false;
  }

  private void closeTimelineStore() {
    if (timelineStore != null) {
      try {
        timelineStore.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the MR3 timeline store", e);
      } finally {
        timelineStore = null;
      }
    }
  }

}
