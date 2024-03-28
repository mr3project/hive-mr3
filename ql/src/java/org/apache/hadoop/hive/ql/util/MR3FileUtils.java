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

package org.apache.hadoop.hive.ql.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.tools.MR3DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

// Reimplementation of FileUtils using MR3
public final class MR3FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MR3FileUtils.class.getName());

  /**
   * Copies files between filesystems.
   */
  public static boolean copy(FileSystem srcFS, Path src,
      FileSystem dstFS, Path dst,
      boolean deleteSource,
      boolean overwrite,
      HiveConf conf) throws IOException {

    boolean copied = false;
    boolean triedDistcp = false;

    /* Run distcp if source file/dir is too big */
    if (srcFS.getUri().getScheme().equals("hdfs")) {
      ContentSummary srcContentSummary = srcFS.getContentSummary(src);
      if (srcContentSummary.getFileCount() > conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES)
              && srcContentSummary.getLength() > conf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE)) {

        LOG.info("Source is " + srcContentSummary.getLength() + " bytes. (MAX: " + conf.getLongVar(
                HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE) + ")");
        LOG.info("Source is " + srcContentSummary.getFileCount() + " files. (MAX: " + conf.getLongVar(
                HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES) + ")");
        LOG.info("Launch distributed copy (distcp) job.");
        triedDistcp = true;
        copied = distCp(srcFS, Collections.singletonList(src), dst, deleteSource, null, conf);
      }
    }
    if (!triedDistcp) {
      // Note : Currently, this implementation does not "fall back" to regular copy if distcp
      // is tried and it fails. We depend upon that behaviour in cases like replication,
      // wherein if distcp fails, there is good reason to not plod along with a trivial
      // implementation, and fail instead.
      copied = FileUtil.copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf);
    }
    return copied;
  }

  public static boolean distCp(FileSystem srcFS, List<Path> srcPaths, Path dst,
      boolean deleteSource, String doAsUser,
      HiveConf conf) throws IOException {
    boolean copied = false;
    if (doAsUser == null){
      copied = runDistCp(srcPaths, dst, conf);
    } else {
      copied = runDistCpAs(srcPaths, dst, conf, doAsUser);
    }
    if (copied && deleteSource) {
      for (Path path : srcPaths) {
        srcFS.delete(path, true);
      }
    }
    return copied;
  }

  // based on org.apache.hadoop.hive.shims.HadoopShims

  private static boolean runDistCpAs(List<Path> srcPaths, Path dst, HiveConf conf, String doAsUser) throws IOException {
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
        doAsUser, UserGroupInformation.getLoginUser());
    try {
      return proxyUser.doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          return runDistCp(srcPaths, dst, conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private static boolean runDistCp(List<Path> srcPaths, Path dst, HiveConf conf) throws IOException {
    DistCpOptions options = new DistCpOptions.Builder(srcPaths, dst)
        .withSyncFolder(true)
        .withCRC(true)
        .preserve(FileAttribute.BLOCKSIZE)
        .build();

    try {
      conf.setBoolean("mapred.mapper.new-api", true);
      MR3DistCp distcp = new MR3DistCp(conf, options);

      // HIVE-13704 states that we should use run() instead of execute() due to a hadoop known issue
      // added by HADOOP-10459
      // For Hive on MR3, we modify execute() to call checkSplitLargeFile() and setTargetPathExists().
      distcp.execute();
      return true;
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: " + e, e);
    } finally {
      conf.setBoolean("mapred.mapper.new-api", false);
    }
  }

  private static final String DISTCP_OPTIONS_PREFIX = "distcp.options.";

  private static List<String> constructDistCpParams(List<Path> srcPaths, Path dst, Configuration conf) {
    List<String> params = new ArrayList<String>();
    for (Map.Entry<String,String> entry : conf.getPropsWithPrefix(DISTCP_OPTIONS_PREFIX).entrySet()){
      String distCpOption = entry.getKey();
      String distCpVal = entry.getValue();
      params.add("-" + distCpOption);
      if ((distCpVal != null) && (!distCpVal.isEmpty())){
        params.add(distCpVal);
      }
    }
    if (params.size() == 0){
      // if no entries were added via conf, we initiate our defaults
      params.add("-update");
      params.add("-pbx");
    }
    for (Path src : srcPaths) {
      params.add(src.toString());
    }
    params.add(dst.toString());
    return params;
  }
}
