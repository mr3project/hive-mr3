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
package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.api.AbstractLogicalInput;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class VectorMapJoinFastHashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastHashTableLoader.class.getName());

  private Configuration hconf;
  protected MapJoinDesc desc;
  private TezContext tezContext;
  private String cacheKey;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext,
      Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    Map<Integer, String> parentToInput = desc.getParentToInput();
    Map<Integer, Long> parentKeyCounts = desc.getParentKeyCounts();

    MemoryMonitorInfo memoryMonitorInfo = desc.getMemoryMonitorInfo();
    boolean doMemCheck = false;
    long effectiveThreshold = 0;
    if (memoryMonitorInfo != null) {
      effectiveThreshold = memoryMonitorInfo.getEffectiveThreshold(desc.getMaxMemoryAvailable());
      if (memoryMonitorInfo.doMemoryMonitoring()) {
        doMemCheck = true;
        if (LOG.isInfoEnabled()) {
          LOG.info("Memory monitoring for hash table loader enabled. {}", memoryMonitorInfo);
        }
      }
    }

    long interruptCheckInterval = HiveConf.getLongVar(hconf, HiveConf.ConfVars.MR3_MAPJOIN_INTERRUPT_CHECK_INTERVAL);
    LOG.info("interruptCheckInterval = " + interruptCheckInterval);

    if (!doMemCheck) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Not doing hash table memory monitoring. {}", memoryMonitorInfo);
      }
    }
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      long numEntries = 0;
      String inputName = parentToInput.get(pos);
      LogicalInput input = tezContext.getInput(inputName);

      try {
        input.start();
        tezContext.getTezProcessorContext().waitForAnyInputReady(
            Collections.<Input> singletonList(input));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();

        Long keyCountObj = parentKeyCounts.get(pos);
        long estKeyCount = (keyCountObj == null) ? -1 : keyCountObj;

        long inputRecords = -1;
        try {
          //TODO : Need to use class instead of string.
          // https://issues.apache.org/jira/browse/HIVE-23981
          inputRecords = ((AbstractLogicalInput) input).getContext().getCounters().
                  findCounter("org.apache.tez.common.counters.TaskCounter",
                          "APPROXIMATE_INPUT_RECORDS").getValue();
        } catch (Exception e) {
          LOG.debug("Failed to get value for counter APPROXIMATE_INPUT_RECORDS", e);
        }
        long keyCount = Math.max(estKeyCount, inputRecords);

        VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer =
                new VectorMapJoinFastTableContainer(desc, hconf, keyCount);

        LOG.info("Loading hash table for input: {} cacheKey: {} tableContainer: {} smallTablePos: {} " +
                "estKeyCount : {} keyCount : {}", inputName, cacheKey,
                vectorMapJoinFastTableContainer.getClass().getSimpleName(), pos, estKeyCount, keyCount);

        vectorMapJoinFastTableContainer.setSerde(null, null); // No SerDes here.
        while (kvReader.next()) {
          vectorMapJoinFastTableContainer.putRow((BytesWritable)kvReader.getCurrentKey(),
              (BytesWritable)kvReader.getCurrentValue());
          numEntries++;
          if ((numEntries % interruptCheckInterval == 0) && Thread.interrupted()) {
            throw new InterruptedException("Hash table loading interrupted");
          }
          if (doMemCheck && (numEntries % memoryMonitorInfo.getMemoryCheckInterval() == 0)) {
              final long estMemUsage = vectorMapJoinFastTableContainer.getEstimatedMemorySize();
              if (estMemUsage > effectiveThreshold) {
                String msg = "Hash table loading exceeded memory limits for input: " + inputName +
                  " numEntries: " + numEntries + " estimatedMemoryUsage: " + estMemUsage +
                  " effectiveThreshold: " + effectiveThreshold + " memoryMonitorInfo: " + memoryMonitorInfo;
                LOG.error(msg);
                throw new MapJoinMemoryExhaustionError(msg);
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Checking hash table loader memory usage for input: {} numEntries: {} " +
                      "estimatedMemoryUsage: {} effectiveThreshold: {}", inputName, numEntries, estMemUsage,
                    effectiveThreshold);
                }
              }
          }
        }

        vectorMapJoinFastTableContainer.seal();
        mapJoinTables[pos] = vectorMapJoinFastTableContainer;
        if (doMemCheck) {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} " +
              "estimatedMemoryUsage: {}", inputName, cacheKey, numEntries,
            vectorMapJoinFastTableContainer.getEstimatedMemorySize());
        } else {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {}",
                  inputName, cacheKey, numEntries);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      } catch (SerDeException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }
}
