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
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3ClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * Simple implementation of <i>MR3SessionManager</i>
 *   - returns MR3Session when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 *   - MR3Session is reused if the userName in new conf and user name in session conf match.
 */
public class MR3SessionManagerImpl implements MR3SessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(MR3SessionManagerImpl.class);

  // guard with synchronize{}
  private HiveConf hiveConf = null;
  private boolean initializedClientFactory = false;
  private boolean initializedSessionManager = false;
  private Set<MR3Session> createdSessions = new HashSet<MR3Session>();

  // 1. serviceDiscovery == true && activePassiveHA == true: multiple HS2 instances, leader exists
  // 2. serviceDiscovery == true && activePassiveHA == false: multiple HS2 instances, no leader
  // 3. serviceDiscovery == false: no ZooKeeper
  private boolean serviceDiscovery = false;
  private boolean activePassiveHA = false;

  private boolean shareMr3Session = false;
  private UserGroupInformation commonUgi = null;
  private SessionState commonSessionState = null;
  private MR3Session commonMr3Session = null;

  private MR3ZooKeeper mr3ZooKeeper = null;

  private String serverUniqueId = null;

  private static MR3SessionManagerImpl instance;

  public static synchronized MR3SessionManagerImpl getInstance() {
    if (instance == null) {
      instance = new MR3SessionManagerImpl();
    }
    return instance;
  }

  // return 'number of Nodes' if taskMemoryInMb == 0
  public static int getEstimateNumTasksOrNodes(int taskMemoryInMb) {
    MR3SessionManagerImpl currentInstance;
    synchronized (MR3SessionManagerImpl.class) {
      if (instance == null) {
        LOG.warn("MR3SessionManager not ready yet, reporting 0 Tasks/Nodes");
        return 0;
      }
      currentInstance = instance;
    }

    MR3Session currentCommonMr3Session;
    synchronized (currentInstance) {
      currentCommonMr3Session = currentInstance.commonMr3Session;
    }
    if (currentCommonMr3Session == null) {
      LOG.warn("No common MR3Session, reporting 0 Tasks/Nodes");
      return 0;
    }

    try {
      return currentCommonMr3Session.getEstimateNumTasksOrNodes(taskMemoryInMb);
    } catch (Exception ex) {
      LOG.error("getEstimateNumTasksOrNodes() failed, reporting 0 Tasks/Nodes");
      return 0;
    }
  }

  public static int getNumNodes() {
    return getEstimateNumTasksOrNodes(0);
  }

  private MR3SessionManagerImpl() {}

  //
  // for HiveServer2
  //

  // called directly from HiveServer2, in which case hiveConf comes from HiveSever2
  // called from MetaStore for compaction
  // MR3SessionManager is provided with zooKeeperClient only once during its lifetime. Even in the case that
  // zooKeeperClient fails, MR3SessionManager can continue to connect to DAGAppMaster. It just cannot call
  // triggerCheckApplicationStatus() any more, so the effect is limited (e.g., other HiveServer2 instances
  // may call triggerCheckApplicationStatus()).
  @Override
  public synchronized boolean setup(
        HiveConf hiveConf, CuratorFramework zooKeeperClient) throws HiveException, IOException {
    // we check initializedSessionManager because setup() can be called from both HiveServer2 and Metastore
    // if Metastore is embedded in HiveServer2 (when hive.metastore.uris is set to an empty string)
    if (initializedSessionManager) {
      return false;
    }

    LOG.info("Setting up MR3SessionManager");
    this.hiveConf = hiveConf;

    HiveMR3ClientFactory.initialize(hiveConf);
    initializedClientFactory = true;

    serviceDiscovery = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY);
    activePassiveHA = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE);

    if (serviceDiscovery && activePassiveHA) {
      if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_MR3_SHARE_SESSION)) {
        LOG.warn("Ignore HIVE_SERVER2_MR3_SHARE_SESSION == false because of active high availability");
      }
      shareMr3Session = true;
      mr3ZooKeeper = new MR3ZooKeeper(hiveConf, zooKeeperClient);
    } else {
      shareMr3Session = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_MR3_SHARE_SESSION);
    }

    LOG.info("Setting up MR3SessionManager: serviceDiscovery/activePassiveHA/shareMr3Session = "
        + serviceDiscovery + "/" + activePassiveHA + "/" + shareMr3Session);

    if (shareMr3Session) {
      // if MR3_SHARE_SESSION is enabled, the scratch directory should be created with permission 733 so that
      // each query can create its own MR3 scratch director, e.g.,
      // ..../<ugi.getShortUserName>/_mr3_scratch_dir-3/
      hiveConf.set(HiveConf.ConfVars.SCRATCH_DIR_PERMISSION.varname, "733");
      commonUgi = UserGroupInformation.getCurrentUser();
      commonSessionState = SessionState.get();
    }

    if (!(serviceDiscovery && activePassiveHA) && shareMr3Session) {
      commonMr3Session = createSession(hiveConf, true);
    } else {
      commonMr3Session = null;  // to be created at the request of HiveServer2
    }

    serverUniqueId = UUID.randomUUID().toString();
    serverUniqueId = serverUniqueId.substring(serverUniqueId.length() - 4);

    initializedSessionManager = true;
    return true;
  }

  //
  // for HiveServer2 with serviceDiscovery == true && activePassiveHA == true
  //

  public synchronized String getCurrentApplication() {
    assert (serviceDiscovery && activePassiveHA);
    assert shareMr3Session;

    if (commonMr3Session != null) {
      return commonMr3Session.getApplicationId().toString();
    } else {
      return null;
    }
  }

  @Override
  public synchronized void setActiveApplication(String appIdStr) throws HiveException {
    assert (serviceDiscovery && activePassiveHA);
    assert shareMr3Session;

    ApplicationId appId = convertToApplicationId(appIdStr);
    if (commonMr3Session != null) {
      if (commonMr3Session.getApplicationId().equals(appId)) {
        LOG.warn("MR3Session already active: " + appId);
      } else {
        LOG.error("Closing current active MR3Session: " + commonMr3Session.getApplicationId());
        commonMr3Session.close(false);
        createdSessions.remove(commonMr3Session);
        commonMr3Session = null;  // connectSession() may raise HiveException

        commonMr3Session = connectSession(this.hiveConf, appId);
      }
    } else {
      commonMr3Session = connectSession(this.hiveConf, appId);
    }
  }

  @Override
  public synchronized void closeApplication(String appIdStr) {
    assert (serviceDiscovery && activePassiveHA);
    assert shareMr3Session;

    ApplicationId appId = convertToApplicationId(appIdStr);
    if (commonMr3Session == null) {
      LOG.warn("No MR3Session running in closeApplication(): " + appId);
    } else {
      if (commonMr3Session.getApplicationId().equals(appId)) {
        LOG.info("Closing Application: " + appId);
        commonMr3Session.close(true);
        createdSessions.remove(commonMr3Session);
        commonMr3Session = null;
      } else {
        LOG.warn("Ignore closeApplication(): " + commonMr3Session.getApplicationId() + " != " + appId);
      }
    }
  }

  @Override
  public synchronized boolean checkIfValidApplication(String appIdStr) {
    assert (serviceDiscovery && activePassiveHA);
    assert shareMr3Session;

    ApplicationId appId = convertToApplicationId(appIdStr);
    if (commonMr3Session == null) {
      LOG.warn("No MR3Session running in closeApplication(): " + appId);
      return false;
    } else {
      if (commonMr3Session.getApplicationId().equals(appId)) {
        return commonMr3Session.isRunningFromApplicationReport();
      } else {
        LOG.warn("Ignore checkIfValidApplication(): " + commonMr3Session.getApplicationId() + " != " + appId);
        return false;
      }
    }
  }

  @Override
  public synchronized String createNewApplication() throws HiveException {
    assert (serviceDiscovery && activePassiveHA);
    assert shareMr3Session;

    if (commonMr3Session != null) {
      LOG.error("Closing current active MR3Session: " + commonMr3Session.getApplicationId());
      commonMr3Session.close(false);
      createdSessions.remove(commonMr3Session);
      commonMr3Session = null;  // createSession() may raise HiveException
    }

    commonMr3Session = createSession(hiveConf, true);
    return commonMr3Session.getApplicationId().toString();
  }

  private ApplicationId convertToApplicationId(String appIdStr) {
    String[] splits = appIdStr.split("_");
    String timestamp = splits[1];
    String id = splits[2];
    return ApplicationId.newInstance(Long.parseLong(timestamp), Integer.parseInt(id));
  }

  //
  // for MR3Task
  //

  @Override
  public synchronized boolean getShareMr3Session() {
    assert initializedClientFactory;  // after setup()

    return shareMr3Session;
  }

  @Override
  public synchronized MR3Session getSession(HiveConf hiveConf) throws HiveException {
    if (!initializedClientFactory) {  // e.g., called from Hive-CLI
      try {
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE, false);
        setup(hiveConf, null);
      } catch (IOException e) {
        throw new HiveException("Error in setting up MR3SessionManager", e);
      }
    }

    if (shareMr3Session) {
      if (commonMr3Session != null) {
        return commonMr3Session;
      } else {
        assert (serviceDiscovery && activePassiveHA);
        // e.g., the previous call to setActiveApplication() may have failed with HiveException
        mr3ZooKeeper.triggerCheckApplicationStatus();
        throw new HiveException("MR3Session not ready yet");
      }
    } else {
      return createSession(hiveConf, false);
    }
  }

  @Override
  public synchronized void closeSession(MR3Session mr3Session) {
    assert !shareMr3Session;

    LOG.info(String.format("Closing MR3Session (%s)", mr3Session.getSessionId()));

    mr3Session.close(true);   // because !shareMr3Session
    createdSessions.remove(mr3Session);
  }

  @Override
  public MR3Session triggerCheckApplicationStatus(MR3Session mr3Session, HiveConf mr3SessionConf)
      throws Exception {
    synchronized (this) {
      if (serviceDiscovery && activePassiveHA) {
        if (commonMr3Session == null) {
          // HiveServer2 is supposed to have called setActiveApplication() to close mr3Session
          return null;  // because there is no other MR3Session to return
        } else if (mr3Session != commonMr3Session) {
          // HiveServer2 is supposed to have called setActiveApplication() to close mr3Session
          return commonMr3Session;
        } else {
          mr3ZooKeeper.triggerCheckApplicationStatus();
          return null;
        }
      }
    }

    return getNewMr3SessionIfNotAlive(mr3Session, mr3SessionConf);
  }

  // if mr3Session is alive, return null
  // if mr3Session is not alive, ***close it*** and return a new one
  // do not update commonMr3Session and raise Exception if a new MR3Session cannot be created
  private MR3Session getNewMr3SessionIfNotAlive(MR3Session mr3Session, HiveConf mr3TaskHiveConf)
      throws HiveException, IOException, InterruptedException {
    boolean isAlive = mr3Session.isRunningFromApplicationReport();
    if (isAlive) {
      LOG.info("MR3Session still alive: " + mr3Session.getSessionId());
      return null;
    } else {
      LOG.info("Closing MR3Session: " + mr3Session.getSessionId());
      // mr3Session.close(): okay to call several times
      // createdSessions.remove() may be executed several times for the same mr3Session if shareMr3Session == true
      synchronized (this) {
        if (shareMr3Session) {
          if (mr3Session == commonMr3Session) {   // reference equality
            SessionState currentSessionState = SessionState.get();  // cache SessionState
            commonUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                SessionState.setCurrentSessionState(commonSessionState);
                MR3Session newMr3Session = new MR3SessionImpl(true, commonUgi.getShortUserName());
                newMr3Session.start(hiveConf);  // may raise Exception
                // assign to commonMr3Session only if newSession.start() returns without raising Exception
                commonMr3Session = newMr3Session;
                return null;
              }
            });
            // now it is safe to close the previous commonMr3Session
            mr3Session.close(true);
            createdSessions.remove(mr3Session);
            // register commonMr3Session
            SessionState.setCurrentSessionState(currentSessionState);   // restore SessionState
            createdSessions.add(commonMr3Session);
            LOG.info("New common MR3Session has been created: " + commonMr3Session.getSessionId());
            return commonMr3Session;
          } else {
            mr3Session.close(true);
            createdSessions.remove(mr3Session);
            LOG.info("New common MR3Session already created: " + commonMr3Session.getSessionId());
            return commonMr3Session;
          }
        } else {
          mr3Session.close(true);
          createdSessions.remove(mr3Session);
          // this is from the thread running MR3Task, so no concurrency issue
          return createSession(mr3TaskHiveConf, false);
        }
      }
    }
  }

  //
  // private methods
  //

  // createSession() is called one at a time because it is in synchronized{}.
  private MR3Session createSession(HiveConf hiveConf, boolean shareSession) throws HiveException {
    String sessionUser = getSessionUser();
    MR3Session mr3Session = new MR3SessionImpl(shareSession, sessionUser);
    mr3Session.start(hiveConf);
    createdSessions.add(mr3Session);

    LOG.info("New MR3Session created: " + mr3Session.getSessionId() + ", " + sessionUser);
    return mr3Session;
  }

  private MR3Session connectSession(HiveConf hiveConf, ApplicationId appId) throws HiveException {
    String sessionUser = getSessionUser();
    MR3Session mr3Session = new MR3SessionImpl(true, sessionUser);
    mr3Session.connect(hiveConf, appId);
    createdSessions.add(mr3Session);

    LOG.info("New MR3Session connected for " + appId + ": " + mr3Session.getSessionId() + ", " + sessionUser);
    return mr3Session;
  }

  private String getSessionUser() throws HiveException {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new HiveException("No session user found", e);
    }
  }

  //
  //
  //

  public String getUniqueId() {
    return serverUniqueId;
  }

  @Override
  public synchronized void shutdown() {
    if (!initializedSessionManager) {
      return;
    }
    LOG.info("Closing MR3SessionManager");
    boolean terminateApplication = !(serviceDiscovery && activePassiveHA);
    if (createdSessions != null) {
      Iterator<MR3Session> it = createdSessions.iterator();
      while (it.hasNext()) {
        MR3Session session = it.next();
        session.close(terminateApplication);
      }
      createdSessions.clear();
    }
    if (mr3ZooKeeper != null) {
      mr3ZooKeeper.close();
      mr3ZooKeeper = null;
    }
    hiveConf = null;
    initializedClientFactory = false;
    initializedSessionManager = false;
  }
}
