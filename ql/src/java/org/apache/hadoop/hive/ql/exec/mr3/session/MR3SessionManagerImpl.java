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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3ClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;

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
  private static final Log LOG = LogFactory.getLog(MR3SessionManagerImpl.class);

  // guard with synchronize{}
  private HiveConf hiveConf = null;
  private boolean initializedClientFactory = false;
  private Set<MR3Session> createdSessions = new HashSet<MR3Session>();

  private boolean shareMr3Session = false;
  private UserGroupInformation commonUgi = null;
  private SessionState commonSessionState = null;
  private MR3Session commonMr3Session = null;

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
  // called from nowhere else
  @Override
  public synchronized void setup(HiveConf hiveConf) throws HiveException, IOException {
    LOG.info("Setting up MR3SessionManager");
    this.hiveConf = hiveConf;

    HiveMR3ClientFactory.initialize(hiveConf);
    initializedClientFactory = true;

    shareMr3Session = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_MR3_SHARE_SESSION);

    if (shareMr3Session) {
      // if MR3_SHARE_SESSION is enabled, the scratch directory should be created with permission 733 so that
      // each query can create its own MR3 scratch director, e.g.,
      // ..../<ugi.getShortUserName>/_mr3_scratch_dir-3/
      hiveConf.set(HiveConf.ConfVars.SCRATCHDIRPERMISSION.varname, "733");
      commonUgi = UserGroupInformation.getCurrentUser();
      commonSessionState = SessionState.get();
    }

    if (shareMr3Session) {
      commonMr3Session = createSession(hiveConf, true);
    } else {
      commonMr3Session = null;
    }

    serverUniqueId = UUID.randomUUID().toString();
    serverUniqueId = serverUniqueId.substring(serverUniqueId.length() - 4);
  }

  // for MR3Task
  //

  @Override
  public synchronized boolean getShareMr3Session() {
    assert initializedClientFactory;  // after setup()

    return shareMr3Session;
  }

  @Override
  public synchronized MR3Session getSession(HiveConf hiveConf) throws HiveException {
    assert initializedClientFactory;  // after setup()

    if (shareMr3Session) {
      return commonMr3Session;
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
      throws HiveException, IOException, InterruptedException {
    return getNewMr3SessionIfNotAlive(mr3Session, mr3SessionConf);
  }

  // if mr3Session is alive, return null
  // if mr3Session is not alive, ***close it*** and return a new one
  private MR3Session getNewMr3SessionIfNotAlive(MR3Session mr3Session, HiveConf mr3TaskHiveConf)
      throws HiveException, IOException, InterruptedException {
    boolean isAlive = mr3Session.isRunningFromApplicationReport();
    if (isAlive) {
      LOG.info("MR3Session still alive: " + mr3Session.getSessionId());
      return null;
    } else {
      LOG.info("Closing MR3Session: " + mr3Session.getSessionId());
      mr3Session.close(true);   // okay to call several times
      synchronized (this) {
        createdSessions.remove(mr3Session);   // may be executed several times for the same mr3Session if shareMr3Session == true

        if (shareMr3Session) {
          if (mr3Session == commonMr3Session) {   // reference equality
            SessionState currentSessionState = SessionState.get();
            commonUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                SessionState.setCurrentSessionState(commonSessionState);
                commonMr3Session = new MR3SessionImpl(true, commonUgi.getShortUserName());
                commonMr3Session.start(hiveConf);
                return null;
              }
            });
            SessionState.setCurrentSessionState(currentSessionState);
            createdSessions.add(commonMr3Session);
            LOG.info("New common MR3Session has been created: " + commonMr3Session.getSessionId());
            return commonMr3Session;
          } else {
            LOG.info("New common MR3Session already created: " + commonMr3Session.getSessionId());
            return commonMr3Session;
          }
        } else {
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
    LOG.info("Closing MR3SessionManager");
    boolean terminateApplication = true;
    if (createdSessions != null) {
      Iterator<MR3Session> it = createdSessions.iterator();
      while (it.hasNext()) {
        MR3Session session = it.next();
        session.close(terminateApplication);
      }
      createdSessions.clear();
    }
  }
}
