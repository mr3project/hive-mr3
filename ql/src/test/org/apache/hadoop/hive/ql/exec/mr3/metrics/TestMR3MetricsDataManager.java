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

package org.apache.hadoop.hive.ql.exec.mr3.metrics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestMR3MetricsDataManager {

  private final MR3MetricsDataManager manager = new MR3MetricsDataManager(null, null, 1);

  @Test
  public void testRejectsNegativeStartTime() {
    assertInvalidTime(-1L, 0L, "startTime must not be negative");
  }

  @Test
  public void testRejectsNegativeEndTime() {
    assertInvalidTime(0L, -1L, "endTime must not be negative");
  }

  @Test
  public void testRejectsReversedTimeRange() {
    assertInvalidTime(2L, 1L, "startTime must not exceed endTime");
  }

  private void assertInvalidTime(long start, long end, String expectedMessage) {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> manager.application("appattempt_1_1_1", start, end, null, null));
    assertEquals(expectedMessage, exception.getMessage());
  }
}
