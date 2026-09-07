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
import java.util.regex.Pattern;

/**
 * Parser for extracting ACL information from Configs
 */
public class ACLConfigurationParser {

  private static final Logger LOG = LoggerFactory.getLogger(ACLConfigurationParser.class);

  // ACLManager, additional users
  private final String HIVE_MR3_UI_VIEW_ACLS = "hive.mr3.ui.view-acls";
  // TODO: currently not used
  private final String HIVE_MR3_UI_MODIFY_ACLS = "hive.mr3.ui.modify-acls";

  private final HiveConf hiveConf;
  private final Map<ACLType, Set<String>> allowedUsers;
  private final Map<ACLType, Set<String>> allowedGroups;
  // split pattern = regular expression that matches one or more consecutive whitespace characters
  private static final Pattern splitPattern = Pattern.compile("\\s+");

  public ACLConfigurationParser(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    allowedUsers = new HashMap<ACLType, Set<String>>(2);
    allowedGroups = new HashMap<ACLType, Set<String>>(2);
    parse();
  }

  private void parse() {
    parseACLType(HIVE_MR3_UI_VIEW_ACLS, ACLType.MR3_UI_VIEW_ACL);
    parseACLType(HIVE_MR3_UI_MODIFY_ACLS, ACLType.MR3_UI_MODIFY_ACL);
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

  private String[] getTrimmedStrings(String str) {
    if (str == null || str.trim().isEmpty()) {
      return new String[0];
    } else {
      return str.trim().split("\\s*,\\s*");
    }
  }
}
