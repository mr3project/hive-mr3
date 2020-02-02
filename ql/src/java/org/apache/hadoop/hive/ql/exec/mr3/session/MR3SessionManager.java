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
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;

/**
 * Defines interface for managing multiple MR3Sessions in Hive when multiple users
 * are executing queries simultaneously on MR3 execution engine.
 */
public interface MR3SessionManager {
  //
  // for HiveServer2
  //

  /**
   * Initialize based on given configuration.
   *
   * @param hiveConf
   */
  boolean setup(HiveConf hiveConf, CuratorFramework zooKeeperClient) throws HiveException, IOException;

  //
  // for HiveServer2 with serviceDiscovery == true && activePassiveHA == true
  //

  // return ApplicationId.toString
  // return null if no ApplicationID is currently available
  // String getCurrentApplication();

  // connect to Application appIdStr
  // if appIdStr is already set in MR3SessionManager, ignore the call
  // if another Application is set, close the connection to it (without terminating it)
  // if unsuccessful, raise HiveException and set the active Application to null
  void setActiveApplication(String appIdStr) throws HiveException;

  // if appIdStr is not found, ignore the call
  // should be called only the owner of Application appIdStr
  // TODO: rename to killApplication()
  // TODO: rename to killActiveApplication()
  void closeApplication(String appIdStr);

  boolean checkIfValidApplication(String appIdStr);

  // return ApplicationId.toString
  String createNewApplication() throws HiveException;

  //
  // for MR3Task
  //

  boolean getShareMr3Session();

  /**
   *
   * @param conf
   * @return MR3Session
   */
  MR3Session getSession(HiveConf conf) throws HiveException;

  /**
   * Close the given session and return it to pool. This is used when the client
   * no longer needs an MR3Session.
   */
  void closeSession(MR3Session mr3Session);

  // if mr3Session is alive or unknown, return null
  // if mr3Session is definitely not alive, ***close it*** and return a new one
  MR3Session triggerCheckApplicationStatus(MR3Session mr3Session, HiveConf mr3SessionConf) throws Exception;

  //
  //
  //

  String getUniqueId();

  /**
   * Shutdown the session manager. Also closing up MR3Sessions in pool.
   */
  void shutdown();
}
