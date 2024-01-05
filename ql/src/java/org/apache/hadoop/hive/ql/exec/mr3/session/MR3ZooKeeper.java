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

package org.apache.hadoop.hive.ql.exec.mr3.session;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.MR3ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MR3ZooKeeper {
  private static Logger LOG = LoggerFactory.getLogger(MR3ZooKeeper.class);

  private String namespacePath;
  private CuratorFramework zooKeeperClient;

  public MR3ZooKeeper(HiveConf hiveConf, CuratorFramework zooKeeperClient) {
    this.zooKeeperClient = zooKeeperClient;
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.MR3_ZOOKEEPER_APPID_NAMESPACE);
    namespacePath = "/" + rootNamespace;
  }

  public void triggerCheckApplicationStatus() {
    String currentTime = new Long(System.currentTimeMillis()).toString();
    String path = namespacePath + MR3ZooKeeperUtils.APP_ID_CHECK_REQUEST_PATH;
    try {
      if (zooKeeperClient.checkExists().forPath(path) == null) {
        zooKeeperClient.create().forPath(path, currentTime.getBytes());
      } else {
        zooKeeperClient.setData().forPath(path, currentTime.getBytes());
      }
    } catch (Exception ex) {
      LOG.error("Failed to create/update ZooKeeper path: " + path, ex);
      // take no further action because triggerCheckApplicationStatus() is likely to be called again from MR3Task
    }
  }

  public void close() {
  }
}
