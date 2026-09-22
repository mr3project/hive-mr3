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

import org.apache.hadoop.hive.ql.exec.mr3.metrics.LeveldbMetricsStore;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsDataManager;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsIngestionService;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsResource;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MetricsStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.AMProxyResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.ATSResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MR3TimelineIngestionService;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.LeveldbTimelineStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.MemoryTimelineStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.ServerResource;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.TimelineDataManager;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.TimelineStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.security.ACLManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.http.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns MR3-UI resources and the active HiveServer2 timeline and metrics writers.
 *
 * MR3-UI reuses the HiveServer2 WebUI connector and security settings.
 */
final class MR3TimelineMetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(MR3TimelineMetricsService.class);
  private static final String UI_INDEX = "hive-webapps/hiveserver2/index.html";

  private final HiveConf conf;

  private TimelineStore timelineStore;
  private volatile TimelineDataManager timelineDataManager;
  private MR3TimelineIngestionService timelineIngestionService;

  private MetricsStore metricsStore;
  // Unlike TimelineDataManager which owns write-side behavior,
  // we do not directly reference MR3MetricsDataManager which is just a read-side facade.
  private MR3MetricsIngestionService metricsIngestionService;

  private boolean enabled;
  private boolean active;

  MR3TimelineMetricsService(HiveConf conf) {
    this.conf = conf;
  }

  synchronized void initialize(HttpServer webServer) throws IOException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_CREATE_SERVER)) {
      LOG.info("MR3-UI is disabled by {}", HiveConf.ConfVars.HIVE_MR3_UI_CREATE_SERVER.varname);
      return;
    }
    validateStaticAssets();

    webServer.addServlet("mr3_ats", "/ats/*", createJerseyServlet(ATSResource.class));
    webServer.addServlet("mr3_proxy", "/proxy/*", createJerseyServlet(AMProxyResource.class));
    webServer.addServlet("mr3_server", "/server/*", createJerseyServlet(ServerResource.class));
    webServer.addServlet("mr3_metrics", "/metrics/mr3/*", createJerseyServlet(MR3MetricsResource.class));

    enabled = true;
  }

  synchronized void activate() throws IOException {
    if (!enabled || active) {
      return;
    }

    try {
      timelineStore = createTimelineStore();
      timelineStore.initialize(conf);

      String adminUser = UserGroupInformation.getCurrentUser().getShortUserName();
      timelineDataManager = TimelineDataManager.createInstance(
          timelineStore, new ACLManager(adminUser, conf));
      timelineDataManager.initialize();

      timelineIngestionService = new MR3TimelineIngestionService(timelineDataManager, conf);
      timelineIngestionService.start();

      metricsStore = createMetricsStore();
      metricsStore.initialize(conf);

      int restMaxPoints = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_METRICS_REST_MAX_POINTS);
      MR3MetricsDataManager.createInstance(
          metricsStore, new ACLManager(adminUser, conf), restMaxPoints);

      metricsIngestionService = new MR3MetricsIngestionService(metricsStore, conf);
      metricsIngestionService.start();

      active = true;
      LOG.info("Activated MR3-UI on this HiveServer2 instance: {}", adminUser);
    } catch (Exception e) {
      deactivateAfterFailure();
      throw new IOException("Failed to start the MR3 timeline and metrics writers", e);
    }
  }

  synchronized void deactivate() {
    if (!active && timelineStore == null && metricsStore == null) {
      return;
    }
    cleanup();
    active = false;
    LOG.info("Deactivated MR3-UI on this HiveServer2 instance");
  }

  synchronized void stop() {
    deactivate();
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

  private MetricsStore createMetricsStore() {
    String storeType = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_METRICS_STORE_TYPE);
    if ("leveldb".equalsIgnoreCase(storeType)) {
      return new LeveldbMetricsStore();
    }
    throw new IllegalArgumentException("Unsupported MR3 metrics store type: " + storeType);
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
    cleanup();
    active = false;
  }

  private void cleanup() {
    if (timelineIngestionService != null) {
      timelineIngestionService.close();
      timelineIngestionService = null;
    }
    if (metricsIngestionService != null) {
      metricsIngestionService.close();
      metricsIngestionService = null;
    }

    TimelineDataManager.clearInstance();
    timelineDataManager = null;
    MR3MetricsDataManager.clearInstance();

    closeTimelineStore();
    closeMetricsStore();
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

  private void closeMetricsStore() {
    if (metricsStore != null) {
      try {
        metricsStore.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop the MR3 metrics store", e);
      } finally {
        metricsStore = null;
      }
    }
  }
}
