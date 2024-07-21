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

package org.apache.hadoop.hive.ql.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ZooKeeperHiveHelper {
  public static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHiveHelper.class.getName());
  public static final String ZOOKEEPER_PATH_SEPARATOR = "/";

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port..
   *
   * @param conf
   **/
  public static String getQuorumServers(HiveConf conf) {
    String[] hosts = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM).split(",");
    String port = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT);
    StringBuilder quorum = new StringBuilder();
    for (int i = 0; i < hosts.length; i++) {
      quorum.append(hosts[i].trim());
      if (!hosts[i].contains(":")) {
        // if the hostname doesn't contain a port, add the configured port to hostname
        quorum.append(":");
        quorum.append(port);
      }

      if (i != hosts.length - 1)
        quorum.append(",");
    }

    return quorum.toString();
  }

  public static CuratorFramework startZooKeeperClient(HiveConf hiveConf, ACLProvider zooKeeperAclProvider,
                                                      boolean addParentNode) throws Exception {
    String zooKeeperEnsemble = getQuorumServers(hiveConf);
    int sessionTimeout =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
            TimeUnit.MILLISECONDS);
    int baseSleepTime =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
            TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    CuratorFramework zkClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .sessionTimeoutMs(sessionTimeout).aclProvider(zooKeeperAclProvider)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zkClient.start();

    if (addParentNode) {
      // Create the parent znodes recursively; ignore if the parent already exists.
      String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
      try {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
        LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper");
      } catch (KeeperException e) {
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          LOG.error("Unable to create namespace: " + rootNamespace + " on ZooKeeper", e);
          throw e;
        }
      }
    }
    return zkClient;
  }

  /**
   * A no-op watcher class
   */
  public static class DummyWatcher implements Watcher {
    @Override
    public void process(org.apache.zookeeper.WatchedEvent event) {
    }
  }
}
