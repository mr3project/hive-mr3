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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.LeveldbMetricsStore;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsDataManager;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsIngestionService;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MR3MetricsResource;
import org.apache.hadoop.hive.ql.exec.mr3.metrics.MetricsStore;
import org.apache.hadoop.hive.ql.exec.mr3.timeline.security.ACLManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.http.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

final class MR3MetricsService {
  private final HiveConf conf;
  private MetricsStore store;
  private MR3MetricsIngestionService ingestion;
  private boolean enabled;

  MR3MetricsService(HiveConf conf) { this.conf = conf; }

  void initialize(HttpServer webServer) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MR3_METRICS_ENABLED)) return;
    ResourceConfig config = new ResourceConfig().register(MR3MetricsResource.class)
        .register(JacksonFeature.class);
    webServer.addServlet("mr3_metrics", "/metrics/mr3/*",
        new ServletHolder(new ServletContainer(config)));
    enabled = true;
  }

  void activate() throws IOException {
    if (!enabled || store != null) return;
    try {
      String type = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MR3_METRICS_STORE_TYPE);
      if (!"leveldb".equalsIgnoreCase(type)) {
        throw new IllegalArgumentException("Unsupported MR3 metrics store type: " + type);
      }
      store = new LeveldbMetricsStore();
      store.initialize(conf);
      String admin = UserGroupInformation.getCurrentUser().getShortUserName();
      MR3MetricsDataManager.setInstance(new MR3MetricsDataManager(store,
          new ACLManager(admin, conf),
          HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_MR3_METRICS_REST_MAX_POINTS)));
      ingestion = new MR3MetricsIngestionService(store, conf);
      ingestion.start();
    } catch (Exception e) {
      deactivate();
      throw new IOException("Failed to start native MR3 metrics service", e);
    }
  }

  void deactivate() {
    if (ingestion != null) ingestion.close();
    ingestion = null;
    MR3MetricsDataManager.setInstance(null);
    if (store != null) {
      try { store.stop(); } catch (Exception ignored) { }
    }
    store = null;
  }
}
