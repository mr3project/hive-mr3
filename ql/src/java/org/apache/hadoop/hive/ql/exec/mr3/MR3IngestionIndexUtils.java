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

package org.apache.hadoop.hive.ql.exec.mr3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class MR3IngestionIndexUtils {

  private static final Pattern CURRENT_START_INDEX_PATTERN =
      Pattern.compile("current start index[^0-9]*(\\d+)", Pattern.CASE_INSENSITIVE);

  private MR3IngestionIndexUtils() {
  }

  /**
   * Returns the new index when MR3 reports that it has already discarded entries before it.
   * Other exception messages, including a stale or malformed start index, are not recoverable.
   */
  public static long getResetIndex(String errorMessage, long fromIndex) {
    assert fromIndex >= 0;
    if (errorMessage == null) {
      return -1L;
    }

    Matcher matcher = CURRENT_START_INDEX_PATTERN.matcher(errorMessage);
    if (!matcher.find()) {
      return -1L;
    }

    try {
      long currentStartIndex = Long.parseLong(matcher.group(1));
      return currentStartIndex > fromIndex ? currentStartIndex : -1L;
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
