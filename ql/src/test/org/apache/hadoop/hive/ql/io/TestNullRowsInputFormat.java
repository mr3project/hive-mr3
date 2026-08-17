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
package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Test;

public class TestNullRowsInputFormat {

  @After
  public void clearIOContext() {
    IOContextMap.clear();
  }

  @Test
  public void testRecordReaderSetsInputPath() throws Exception {
    JobConf conf = new JobConf();
    Path inputPath = new Path("file:/__hive_dummy__/null");

    new NullRowsInputFormat.NullRowsRecordReader(
        conf, new NullRowsInputFormat.DummyInputSplit(inputPath));

    assertEquals(inputPath, IOContextMap.get(conf).getInputPath());
  }
}
