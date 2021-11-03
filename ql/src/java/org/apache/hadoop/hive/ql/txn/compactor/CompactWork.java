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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class CompactWork extends BaseWork {
  JobConf jobConf;
  public CompactWork(JobConf jobConf) {
    super();
    this.jobConf = jobConf;
  }

  @Override
  public String getName() {
    return "Compaction";
  }

  @Override
  public void configureJobConf(JobConf job) {
    // Do not set "tmpjars" because hive-exec.jar is already included in sessionLocalResources.
    job.setCredentials(jobConf.getCredentials());
  }

  public JobConf configureVertexConf(JobConf jobConf) {
    return this.jobConf;
  }


  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
  }

  @Override
  public Set<Operator<? extends OperatorDesc>> getAllRootOperators() {
    return Collections.emptySet();
  }

  @Override
  public Operator<? extends OperatorDesc> getAnyRootOperator() {
    return null;
  }

}
