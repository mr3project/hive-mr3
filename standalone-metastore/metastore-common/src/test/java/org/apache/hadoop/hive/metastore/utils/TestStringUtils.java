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

package org.apache.hadoop.hive.metastore.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestStringUtils {
  @Test
  public void testInternCanonicalizesEqualStrings() {
    String first = new String("partition_parameter");
    String second = new String("partition_parameter");

    Assert.assertNotSame(first, second);
    Assert.assertSame(StringUtils.intern(first), StringUtils.intern(second));
  }

  @Test
  public void testInternMapCanonicalizesKeysOnly() {
    Map<String, String> map = new HashMap<>();
    String value = new String("value");
    map.put(new String("key"), value);

    Map<String, String> interned = StringUtils.intern(map);

    Assert.assertSame(StringUtils.intern("key"), interned.keySet().iterator().next());
    Assert.assertSame(value, interned.values().iterator().next());
  }
}
