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

package org.apache.hadoop.hive.ql.exec.mr3.timeline.security;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class to manage ACLs and provides functionality to check whether
 * a user is authorized to take certain actions.
 */
public class ACLManager {

  private static final Logger LOG = LoggerFactory.getLogger(ACLManager.class);

  public static final String WILDCARD_ACL_VALUE = "*";

  private final String adminUser;
  private final boolean aclsEnabled;

  private final Map<ACLType, Set<String>> users;
  private final Map<ACLType, Set<String>> groups;

  public ACLManager(String adminUser, HiveConf hiveConf) {
    this.adminUser = adminUser;
    this.aclsEnabled = HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MR3_UI_ACLS_ENABLED);

    this.users = new HashMap<ACLType, Set<String>>();
    this.groups = new HashMap<ACLType, Set<String>>();

    if (aclsEnabled) {
      ACLConfigurationParser parser = new ACLConfigurationParser(hiveConf);
      if (parser.getAllowedUsers() != null) {
        this.users.putAll(parser.getAllowedUsers());
      }
      if (parser.getAllowedGroups() != null) {
        this.groups.putAll(parser.getAllowedGroups());
      }
    }
  }

  public boolean isAclsEnabled() {
    return aclsEnabled;
  }

  public boolean checkAccess(UserGroupInformation ugi, ACLType aclType) {
    if (!aclsEnabled) {
      return true;
    }

    String user = ugi.getShortUserName();
    if (adminUser.equals(user)) {
      return true;
    }

    if (users != null && !users.isEmpty()) {
      Set<String> set = users.get(aclType);
      if (set != null) {
        if (set.contains(WILDCARD_ACL_VALUE)) {
          return true;
        }
        if (set.contains(user)) {
          return true;
        }
      }
    }

    Collection<String> userGroups = Arrays.asList(ugi.getGroupNames());
    if (!userGroups.isEmpty() && groups != null && !groups.isEmpty()) {
      Set<String> set = groups.get(aclType);
      if (set != null) {
        for (String userGrp : userGroups) {
          if (set.contains(userGrp)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean checkAMViewAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_VIEW_ACL);
  }

  public boolean checkAMModifyAccess(UserGroupInformation ugi) {
    return checkAccess(ugi, ACLType.AM_MODIFY_ACL);
  }
}
