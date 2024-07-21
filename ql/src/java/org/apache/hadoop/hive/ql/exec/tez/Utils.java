/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hive.common.util.Murmur3;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.slf4j.Logger;

public class Utils {

  public static SplitLocationProvider getSplitLocationProvider(Configuration conf, Logger LOG)
      throws IOException {
    // fall back to checking confs
    return getSplitLocationProvider(conf, true, LOG);
  }

  public static SplitLocationProvider getSplitLocationProvider(Configuration conf,
                                                               boolean useCacheAffinity,
                                                               Logger LOG) throws IOException {
    return getSplitLocationProvider(conf, useCacheAffinity, null, LOG);
  }

  public static SplitLocationProvider getSplitLocationProvider(Configuration conf,
                                                               boolean useCacheAffinity,
                                                               InputInitializerContext context,
                                                               Logger LOG) throws IOException {
    boolean useCustomLocations =
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE).equals("llap")
        && HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS) 
        && useCacheAffinity
        && context != null;
    SplitLocationProvider splitLocationProvider;
    LOG.info("SplitGenerator using llap affinitized locations: " + useCustomLocations);
    if (useCustomLocations) {
      splitLocationProvider = new SplitLocationProvider() {
        @Override
        public String[] getLocations(InputSplit split) throws IOException {
          if (!(split instanceof FileSplit)) {
            return split.getLocations();
          }
          FileSplit fsplit = (FileSplit) split;
          long hash = hash1(getHashInputForSplit(fsplit.getPath().toString(), fsplit.getStart()));
          String location = context.getLocationHintFromHash(hash);
          if (LOG.isDebugEnabled()) {
            String splitDesc = "Split at " + fsplit.getPath() + " with offset=" + fsplit.getStart();
            LOG.debug(splitDesc + " mapped to location=" + location);
          }
          return (location != null) ? new String[] { location } : null;
        }

        private byte[] getHashInputForSplit(String path, long start) {
          // Explicitly using only the start offset of a split, and not the length. Splits generated on
          // block boundaries and stripe boundaries can vary slightly. Try hashing both to the same node.
          // There is the drawback of potentially hashing the same data on multiple nodes though, when a
          // large split is sent to 1 node, and a second invocation uses smaller chunks of the previous
          // large split and send them to different nodes.
          byte[] pathBytes = path.getBytes();
          byte[] allBytes = new byte[pathBytes.length + 8];
          System.arraycopy(pathBytes, 0, allBytes, 0, pathBytes.length);
          SerDeUtils.writeLong(allBytes, pathBytes.length, start >> 3);
          return allBytes;
        }

        private long hash1(byte[] bytes) {
          final int PRIME = 104729; // Same as hash64's default seed.
          return Murmur3.hash64(bytes, 0, bytes.length, PRIME);
        }

        @Override
        public String toString() {
          return "LLAP SplitLocationProvider";
        }
      };
    } else {
      splitLocationProvider = new SplitLocationProvider() {
        @Override
        public String[] getLocations(InputSplit split) throws IOException {
          if (split == null) {
            return null;
          }
          String[] locations = split.getLocations();
          if (locations != null && locations.length == 1) {
            if ("localhost".equals(locations[0])) {
              return ArrayUtils.EMPTY_STRING_ARRAY;
            }
          }
          return locations;
        }

        @Override
        public String toString() {
          return "Default SplitLocationProvider";
        }
      };
    }
    return splitLocationProvider;
  }
}
