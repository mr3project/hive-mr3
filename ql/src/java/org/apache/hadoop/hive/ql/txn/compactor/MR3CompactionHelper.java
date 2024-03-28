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

package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.MR3Task;
import org.apache.hadoop.hive.ql.exec.mr3.MR3ZooKeeperUtils;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapReduceMapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MR3CompactionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(MR3CompactionHelper.class);

  private HiveConf hiveConf;
  private boolean highAvailabilityEnabled;

  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {
    @Override
    public List<ACL> getDefaultAcl() {
      List<ACL> nodeAcls = new ArrayList<ACL>();
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  MR3CompactionHelper(HiveConf hiveConf) throws IOException, HiveException {
    this.hiveConf = hiveConf;
    this.highAvailabilityEnabled =
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY) &&
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE);

    if (SessionState.get() == null) {
      SessionState.start(hiveConf);
    }
    trySetupMr3SessionManager(hiveConf);
  }

  public void submitJobToMr3(JobConf jobConf)
          throws IOException, HiveException {
    if (highAvailabilityEnabled) {
      // If this Metastore does not run inside HiveServer2 process, we should explicitly set appId.
      // If it runs inside HiveServer2 process (hive.metastore.runworker.in=hiveserver2), appId is set
      // by HiveServer2, so the following block is skipped.
      if (!HiveConf.isLoadHiveServer2Config()) {
        String appId = getAppIdFromZooKeeper(hiveConf);
        MR3SessionManagerImpl.getInstance().setActiveApplication(appId);
      }
    }

    jobConf.setCredentials(UserGroupInformation.getCurrentUser().getCredentials());
    TezWork tezWork = createTezWork(jobConf);
    MR3Task mr3Task = new MR3Task(hiveConf, new SessionState.LogHelper(LOG), new AtomicBoolean(false));
    int returnCode = mr3Task.execute(null, tezWork);  // blocking

    if (returnCode != 0) {
      throw new HiveException("Compaction using MR3 failed", mr3Task.getException());
    }
  }

  private String getAppIdFromZooKeeper(HiveConf hiveConf) throws IOException, HiveException {
    CuratorFramework zooKeeperClient = startZooKeeperClient(hiveConf);

    String namespacePath = "/" + HiveConf.getVar(hiveConf, HiveConf.ConfVars.MR3_ZOOKEEPER_APPID_NAMESPACE);
    InterProcessMutex appIdLock =
            new InterProcessMutex(zooKeeperClient, namespacePath + MR3ZooKeeperUtils.APP_ID_LOCK_PATH);
    String appId;

    try {
      appIdLock.acquire();
      appId = new String(zooKeeperClient.getData().forPath(namespacePath + MR3ZooKeeperUtils.APP_ID_PATH));
    } catch (Exception e) {
      throw new IOException("Cannot connect to zookeeper", e);
    } finally {
      try {
        if (appIdLock.isAcquiredInThisProcess()) {
          appIdLock.release();
        }
      } catch (Exception e) {
        LOG.warn("Failed to unlock appIdLock", e);
      } finally {
        zooKeeperClient.close();
      }
    }

    return appId;
  }

  private CuratorFramework startZooKeeperClient(HiveConf hiveConf) throws IOException, HiveException {
    try {
      // TODO: Why metastore/hiveserver2 of Hive4 does not call setUpZooKeeperAuth()?
      // Cf. do not call setUpZooKeeperAuth(), unlike in Hive 3
      return hiveConf.getZKConfig().startZookeeperClient(zooKeeperAclProvider, false);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {   // because Curator throws Exception instead of IOException
      throw new IOException("Failed to start ZooKeeperClient", e);
    }
  }

  private void setUpZooKeeperAuth(HiveConf hiveConf) throws IOException, HiveException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
      if (principal.isEmpty()) {
        throw new HiveException("Metastore Kerberos principal is empty");
      }
      String keyTabFile = hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE);
      if (keyTabFile.isEmpty()) {
        throw new HiveException("Metastore Kerberos keytab is empty");
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
    }
  }

  private void trySetupMr3SessionManager(HiveConf hiveConf) throws IOException, HiveException {
    if (highAvailabilityEnabled) {
      CuratorFramework zooKeeperClientForMr3 = startZooKeeperClient(hiveConf);
      if (MR3SessionManagerImpl.getInstance().setup(hiveConf, zooKeeperClientForMr3)) {
        ShutdownHookManager.addShutdownHook(() -> {
          MR3SessionManagerImpl.getInstance().shutdown();
          zooKeeperClientForMr3.close();
        });
      } else {
        zooKeeperClientForMr3.close();
      }
    } else {
      if (MR3SessionManagerImpl.getInstance().setup(hiveConf, null)) {
        ShutdownHookManager.addShutdownHook(() -> {
          MR3SessionManagerImpl.getInstance().shutdown();
        });
      }
    }
  }

  private TezWork createTezWork(JobConf jobConf) {
    MapReduceMapWork compactionWork = new MapReduceMapWork(jobConf, "Compaction");
    TezWork tezWork = new TezWork(jobConf.getJobName(), jobConf);
    tezWork.add(compactionWork);
    return tezWork;
  }
}
