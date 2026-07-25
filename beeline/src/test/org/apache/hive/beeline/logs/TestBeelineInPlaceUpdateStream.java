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
package org.apache.hive.beeline.logs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hive.jdbc.logs.InPlaceUpdateStream;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.junit.Test;

public class TestBeelineInPlaceUpdateStream {

  @Test
  public void testCompletedProgressRenderedBeforeOperationLog() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BeelineInPlaceUpdateStream stream = new BeelineInPlaceUpdateStream(new PrintStream(output),
        new InPlaceUpdateStream.EventNotifier());

    stream.update(progressResponse(TJobExecutionStatus.COMPLETE));

    assertTrue(new String(output.toByteArray(), UTF_8).contains("ELAPSED TIME"));
  }

  @Test
  public void testInProgressUpdateWaitsForOperationLog() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BeelineInPlaceUpdateStream stream = new BeelineInPlaceUpdateStream(new PrintStream(output),
        new InPlaceUpdateStream.EventNotifier());

    stream.update(progressResponse(TJobExecutionStatus.IN_PROGRESS));

    assertFalse(new String(output.toByteArray(), UTF_8).contains("ELAPSED TIME"));
  }

  private TProgressUpdateResp progressResponse(TJobExecutionStatus status) {
    return new TProgressUpdateResp(
        Arrays.asList("VERTICES", "MODE", "STATUS", "TOTAL", "COMPLETED", "RUNNING",
            "PENDING", "FAILED", "KILLED"),
        Collections.singletonList(Arrays.asList("Map 1", "container", "SUCCEEDED", "1", "1",
            "0", "0", "0", "0")),
        1.0,
        status,
        "VERTICES: 01/01",
        System.currentTimeMillis());
  }
}
