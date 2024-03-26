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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

//
public class MapReduceMapWork extends BaseWork {
  private JobConf jobConf;
  private String name;

  public MapReduceMapWork(JobConf jobConf, String name) {
    super();
    this.jobConf = jobConf;
    this.name = name;
  }

  @Override
  public String getName() {
    return "MapReduceMapWork_" + name;
  }

  @Override
  public void configureJobConf(JobConf job) {
    job.setCredentials(jobConf.getCredentials());
  }

  public JobConf configureVertexConf(JobConf jobConf) {
    return this.jobConf;  // ignore jobConf and return this.jobConf
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
