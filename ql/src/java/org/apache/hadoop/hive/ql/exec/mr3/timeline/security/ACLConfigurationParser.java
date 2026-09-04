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

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * Parser for extracting ACL information from Configs
 */
public class ACLConfigurationParser {

  private static final Logger LOG = LoggerFactory.getLogger(ACLConfigurationParser.class);

  // ACLManager, additional AM users
  private final String MR3_AM_VIEW_ACLS = "mr3.am.view-acls";
  private final String MR3_AM_MODIFY_ACLS = "mr3.am.modify-acls";

  // ACLManager, additional DAG users
  private final String MR3_AM_DAG_VIEW_ACLS = "mr3.am.dag.view-acls";
  private final String MR3_AM_DAG_MODIFY_ACLS = "mr3.am.dag.modify-acls";

  private final HiveConf hiveConf;
  private final Map<ACLType, Set<String>> allowedUsers;
  private final Map<ACLType, Set<String>> allowedGroups;
  // split pattern = regular expression that matches one or more consecutive whitespace characters
  private static final Pattern splitPattern = Pattern.compile("\\s+");

  public ACLConfigurationParser(HiveConf hiveConf) {
    this(hiveConf, false);
  }

  public ACLConfigurationParser(HiveConf hiveConf, boolean dagACLs) {
    this.hiveConf = hiveConf;
    allowedUsers = new HashMap<ACLType, Set<String>>(2);
    allowedGroups = new HashMap<ACLType, Set<String>>(2);
    parse(dagACLs);
  }

  private void parse(boolean dagACLs) {
    if (!dagACLs) {
      parseACLType(MR3_AM_VIEW_ACLS, ACLType.AM_VIEW_ACL);
      parseACLType(MR3_AM_MODIFY_ACLS, ACLType.AM_MODIFY_ACL);
    } else {
      parseACLType(MR3_AM_DAG_VIEW_ACLS, ACLType.DAG_VIEW_ACL);
      parseACLType(MR3_AM_DAG_MODIFY_ACLS, ACLType.DAG_MODIFY_ACL);
    }
  }

  private boolean isWildCard(String aclStr) {
    return aclStr.trim().equals(ACLManager.WILDCARD_ACL_VALUE);
  }

  private void parseACLType(String configProperty, ACLType aclType) {
    String aclsStr = hiveConf.get(configProperty);
    if (aclsStr == null || aclsStr.isEmpty()) {
      return;
    }
    if (isWildCard(aclsStr)) {
      allowedUsers.put(aclType, Sets.newHashSet(ACLManager.WILDCARD_ACL_VALUE));
      return;
    }

    final String[] splits = splitPattern.split(aclsStr);
    int counter = -1;
    String userListStr = null;
    String groupListStr = null;
    for (String s : splits) {
      if (s.isEmpty()) {
        if (userListStr != null) {
          continue;
        }
      }
      ++counter;
      if (counter == 0) {
        userListStr = s;
      } else if (counter == 1) {
        groupListStr = s;
      } else {
        LOG.warn("Invalid configuration specified for " + configProperty
            + ", ignoring configured ACLs, value=" + aclsStr);
        return;
      }
    }

    if (userListStr == null) {
      return;
    }
    if (!userListStr.isEmpty()) {
      allowedUsers.put(aclType,
          Sets.newLinkedHashSet(Arrays.asList(getTrimmedStrings(userListStr))));
    }
    if (groupListStr != null && !groupListStr.isEmpty()) {
      allowedGroups.put(aclType,
          Sets.newLinkedHashSet(Arrays.asList(getTrimmedStrings(groupListStr))));
    }

  }

  public Map<ACLType, Set<String>> getAllowedUsers() {
    return Collections.unmodifiableMap(allowedUsers);
  }

  public Map<ACLType, Set<String>> getAllowedGroups() {
    return Collections.unmodifiableMap(allowedGroups);
  }

  public void addAllowedUsers(Map<ACLType, Set<String>> additionalAllowedUsers) {
    for (Entry<ACLType, Set<String>> entry : additionalAllowedUsers.entrySet()) {
      if (allowedUsers.containsKey(entry.getKey())) {
        allowedUsers.get(entry.getKey()).addAll(entry.getValue());
      } else {
        allowedUsers.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public void addAllowedGroups(Map<ACLType, Set<String>> additionalAllowedGroups) {
    for (Entry<ACLType, Set<String>> entry : additionalAllowedGroups.entrySet()) {
      if (allowedGroups.containsKey(entry.getKey())) {
        allowedGroups.get(entry.getKey()).addAll(entry.getValue());
      } else {
        allowedGroups.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private String[] getTrimmedStrings(String str) {
    if (str == null || str.trim().isEmpty()) {
      return new String[0];
    } else {
      return str.trim().split("\\s*,\\s*");
    }
  }
}
