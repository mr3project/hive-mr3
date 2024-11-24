/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.data;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.InputFile;

public class CachingDeleteLoader extends BaseDeleteLoader {
  private final ObjectCache cache;

  public CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile, Configuration conf) {
    super(loadInputFile);

    // return per-query cache
    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
    int dagIdId = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID);
    if (dagIdId == HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID.defaultIntVal) {
      this.cache = null;
    } else {
      this.cache = ObjectCacheFactory.getCache(conf, queryId, dagIdId, false, true);   // use per-query cache
    }
  }

  @Override
  protected boolean canCache(long size) {
    return cache != null;
  }

  @Override
  protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
    try {
      return cache.retrieve(key, valueSupplier::get);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }
}
