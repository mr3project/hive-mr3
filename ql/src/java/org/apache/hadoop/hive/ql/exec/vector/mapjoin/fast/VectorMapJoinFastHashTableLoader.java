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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.api.KeyValueReaderEdge;
import org.apache.tez.runtime.library.api.LogicalInputEdge;
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
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.slf4j.MDC;

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
  private TezCounter htLoadCounter;
  private LongAccumulator totalEntries;

  // Parallel loading variables
  private int numLoadThreads;
  private ExecutorService loadExecService;
  private List<Future<?>> loadTasks;
  private HashTableElementBatch[] elementBatches;
  private FixedSizedObjectPool<HashTableElementBatch> batchPool;
  private BlockingQueue<HashTableElementBatch>[] loadBatchQueues;
  private static final HashTableElementBatch DONE_SENTINEL = new HashTableElementBatch();

  public VectorMapJoinFastHashTableLoader() {

  }

  public VectorMapJoinFastHashTableLoader(TezContext context, Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = context;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    this.htLoadCounter = this.tezContext.getTezProcessorContext().getCounters().findCounter(
        HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP), hconf.get(Operator.CONTEXT_NAME_KEY, ""));
  }

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext,
      Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    String counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP);
    String vertexName = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    String counterName = Utilities.getVertexCounterName(HashTableLoaderCounters.HASHTABLE_LOAD_TIME_MS.name(), vertexName);
    this.htLoadCounter = tezContext.getTezProcessorContext().getCounters().findCounter(counterGroup, counterName);
  }

  private void initHTLoadingService(long estKeyCount) {
    if (estKeyCount < VectorMapJoinFastHashTable.FIRST_SIZE_UP) {
      // Avoid many small HTs that will rehash multiple times causing GCs
      this.numLoadThreads = 1;
    } else {
      int initialValue = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_MAPJOIN_PARALEL_HASHTABLE_THREADS);
      Preconditions.checkArgument(initialValue > 0, "The number of HT-loading-threads should be positive.");

      int adjustedValue = Integer.highestOneBit(initialValue);
      if (initialValue != adjustedValue) {
        LOG.info("Adjust the number of HT-loading-threads to {}. (Previous value: {})",
            adjustedValue, initialValue);
      }

      this.numLoadThreads = adjustedValue;
    }
    this.totalEntries = new LongAccumulator(Long::sum, 0L);
    this.loadExecService = Executors.newFixedThreadPool(numLoadThreads,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setPriority(Thread.NORM_PRIORITY)
            .setNameFormat("HT-Load-Thread-%d")
            .build());
    this.loadTasks = new ArrayList<>(numLoadThreads);
    // Reuse HashTableElementBatches to reduce GC pressure
    this.batchPool = new FixedSizedObjectPool<>(8 * numLoadThreads, new Pool.PoolObjectHelper<HashTableElementBatch>() {
      @Override
      public HashTableElementBatch create() {
        return new HashTableElementBatch();
      }

      @Override
      public void resetBeforeOffer(HashTableElementBatch elementBatch) {
        elementBatch.reset();
      }
    });
    this.elementBatches = new HashTableElementBatch[numLoadThreads];
    this.loadBatchQueues = new BlockingQueue[numLoadThreads];
    for (int i = 0; i < numLoadThreads; i++) {
      loadBatchQueues[i] = new LinkedBlockingQueue();
      elementBatches[i] = batchPool.take();
    }
  }

  private void submitQueueDrainThreads(VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer)
      throws InterruptedException, IOException, SerDeException {
    for (int partitionId = 0; partitionId < numLoadThreads; partitionId++) {
      int finalPartitionId = partitionId;
      this.loadTasks.add(this.loadExecService.submit(() -> {
        Map<String, String> oldMdcContext = MDC.getCopyOfContextMap();
        try {
          org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.restoreMdc(
              tezContext.getTezProcessorContext().getMdcContext());
          LOG.info("Partition id {} with Queue size {}", finalPartitionId, loadBatchQueues[finalPartitionId].size());
          drainAndLoadForPartition(finalPartitionId, vectorMapJoinFastTableContainer);
        } catch (IOException | InterruptedException | SerDeException | HiveException e) {
          throw new RuntimeException("Failed to start HT Load threads", e);
        } finally {
          org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.restoreMdc(oldMdcContext);
        }
      }));
    }
  }

  private void drainAndLoadForPartition(int partitionId, VectorMapJoinFastTableContainer tableContainer)
      throws InterruptedException, IOException, HiveException, SerDeException {
    LOG.info("Starting draining thread {}", partitionId);
    long totalProcessedEntries = 0;
    while (true) {
      HashTableElementBatch batch = this.loadBatchQueues[partitionId].take();
      if (batch == DONE_SENTINEL) {
        break;
      }
      LOG.debug("Draining thread {} batchSize {}", partitionId, batch.getSize());
      for (int i = 0; i < batch.getSize(); i++) {
        try {
          HashTableElement h = batch.getBatch(i);
          h.putRow(tableContainer);
        }
        catch (Exception e) {
          throw new HiveException("Exception in draining thread put row", e);
        }
      }
      totalProcessedEntries += batch.getSize();
      LOG.debug("Draining thread {} added {} entries", partitionId, batch.getSize());
      totalEntries.accumulate(batch.getSize());
      // Offer must be at the end as it is resetting the Index(size)
      this.batchPool.offer(batch);
    }

    LOG.info("Terminating draining thread {} after processing Entries {}", partitionId, totalProcessedEntries);
  }

  private void addQueueDoneSentinel() {
    // Add sentinel at the end of queue
    for (int i = 0; i < numLoadThreads; i++) {
      // add sentinel to the Queue not the batch!
      loadBatchQueues[i].add(elementBatches[i]);
      loadBatchQueues[i].add(DONE_SENTINEL);
    }
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
        LOG.info("Memory monitoring for hash table loader enabled. {}", memoryMonitorInfo);
      }
    }

    long interruptCheckInterval = HiveConf.getLongVar(hconf, HiveConf.ConfVars.MR3_MAPJOIN_INTERRUPT_CHECK_INTERVAL);
    LOG.info("VectorMapJoin interruptCheckInterval = " + interruptCheckInterval);

    if (!doMemCheck) {
      LOG.info("Not doing hash table memory monitoring. {}", memoryMonitorInfo);
    }
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      String inputName = parentToInput.get(pos);
      LogicalInputEdge input = (LogicalInputEdge)tezContext.getInput(inputName);

      try {
        input.start();
        tezContext.getTezProcessorContext().waitForAnyInputReady(
            Collections.<Input> singletonList(input));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      try {
        KeyValueReaderEdge kvReader = (KeyValueReaderEdge) input.getReader();
        boolean batchInput = desc.isVectorMapJoinBatchInput(pos);
        HashTableKeyType keyType = ((VectorMapJoinDesc) desc.getVectorDesc()).getHashTableKeyType();
        boolean nativeLongKey = keyType != HashTableKeyType.STRING && keyType != HashTableKeyType.MULTI_KEY;
        VectorMapJoinFastBatchDeserializer batchReader = batchInput
            ? new VectorMapJoinFastBatchDeserializer(keyType) : null;

        Long keyCountObj = parentKeyCounts.get(pos);
        long estKeyCount = (keyCountObj == null) ? -1 : keyCountObj;

        long inputRecords = -1;
        try {
          inputRecords = ((AbstractLogicalInput) input).getContext().getCounters()
            .findCounter(TaskCounter.APPROXIMATE_INPUT_RECORDS)
            .getValue();
        } catch (Exception e) {
          LOG.debug("Failed to get value for counter APPROXIMATE_INPUT_RECORDS", e);
        }
        if (batchInput && inputRecords >= 0) {
          // This counter measures outer records. Frames contain at most DEFAULT_SIZE rows;
          // partial frames and ordinary fallback records make this a conservative estimate.
          inputRecords = inputRecords > Long.MAX_VALUE / VectorizedRowBatch.DEFAULT_SIZE
              ? Long.MAX_VALUE : inputRecords * VectorizedRowBatch.DEFAULT_SIZE;
        }
        long keyCount = Math.max(estKeyCount, inputRecords);
        initHTLoadingService(keyCount);

        VectorMapJoinFastTableContainer tableContainer =
            new VectorMapJoinFastTableContainer(desc, hconf, keyCount, numLoadThreads);

        LOG.info("Loading hash table for input: {} cacheKey: {} tableContainer: {} smallTablePos: {} " +
                "estKeyCount : {} keyCount : {}", inputName, cacheKey,
                tableContainer.getClass().getSimpleName(), pos, estKeyCount, keyCount);

        tableContainer.setSerde(null, null); // No SerDes here.
        // Submit parallel loading Threads
        submitQueueDrainThreads(tableContainer);

        long receivedEntries = 0;
        long startTime = System.currentTimeMillis();
        while (kvReader.next()) {
          // currentKey, currentValue: backing byte[] arrays of BytesWritable are immutable.
          BytesWritable currentKey = kvReader.getCurrentKey();
          BytesWritable currentValue = kvReader.getCurrentValue();

          boolean isBatch = VectorMapJoinFastBatch.isBatchKey(currentKey);
          if (isBatch) {
            if (!batchInput) {
              throw new IOException("Unexpected map-join batch for input " + inputName);
            }
            batchReader.set(currentValue);
          }
          do {
            if (isBatch && !batchReader.next()) {
              break;
            }
            HashTableElement h;
            if (isBatch) {
              h = new HashTableElement(batchReader, nativeLongKey);
            } else {
              h = new HashTableElement(tableContainer.getHashCode(currentKey),
                  currentKey.getBytesRaw(), currentKey.getOffset(), currentKey.getLength(),
                  currentValue.getBytesRaw(), currentValue.getOffset(), currentValue.getLength());
            }

            int partitionId = (int) ((numLoadThreads - 1) & h.hashCode);
            if (elementBatches[partitionId].addElement(h)) {
              loadBatchQueues[partitionId].add(elementBatches[partitionId]);
              elementBatches[partitionId] = batchPool.take();
            }
            receivedEntries++;
            if ((receivedEntries % interruptCheckInterval == 0) && Thread.interrupted()) {
              throw new InterruptedException("Hash table loading interrupted");
            }
            if (doMemCheck && (receivedEntries % memoryMonitorInfo.getMemoryCheckInterval() == 0)) {
              final long estMemUsage = tableContainer.getEstimatedMemorySize();
              if (estMemUsage > effectiveThreshold) {
                String msg = "Hash table loading exceeded memory limits for input: " + inputName +
                    " numEntries: " + receivedEntries + " estimatedMemoryUsage: " + estMemUsage +
                    " effectiveThreshold: " + effectiveThreshold + " memoryMonitorInfo: " + memoryMonitorInfo;
                LOG.error(msg);
                throw new MapJoinMemoryExhaustionError(msg);
              } else {
                if (LOG.isDebugEnabled()) { LOG.debug(
                    "Checking hash table loader memory usage for input: {} numEntries: {} "
                        + "estimatedMemoryUsage: {} effectiveThreshold: {}",
                    inputName, receivedEntries, estMemUsage, effectiveThreshold); }
              }
            }
          } while (isBatch);
        }

        LOG.info("Finished loading the queue for input: {} waiting {} minutes for TPool shutdown", inputName, 2);
        addQueueDoneSentinel();
        loadExecService.shutdown();

        if (!loadExecService.awaitTermination(2, TimeUnit.MINUTES)) {
          throw new HiveException("Failed to complete the hash table loader. Loading timed out.");
        }
        for (Future<?> loadTask : loadTasks) {
          loadTask.get();
        }
        assert totalEntries.get() == receivedEntries;
        batchPool.clear();
        LOG.info("Total received entries: {} Threads {} HT entries: {}", receivedEntries, numLoadThreads, totalEntries.get());

        long delta = System.currentTimeMillis() - startTime;
        htLoadCounter.increment(delta);

        tableContainer.seal();
        mapJoinTables[pos] = tableContainer;
        if (doMemCheck) {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} " +
              "estimatedMemoryUsage: {} Load Time : {} ", inputName, cacheKey, receivedEntries,
            tableContainer.getEstimatedMemorySize(), delta);
        } else {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} Load Time : {} ",
                  inputName, cacheKey, receivedEntries, delta);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HiveException(e);
      } catch (HiveException e) {
        throw e;
      } catch (Exception e) {
        throw new HiveException(e);
      } finally {
        if (loadExecService != null && !loadExecService.isTerminated()) {
          loadExecService.shutdownNow();
        }
      }
    }
  }

  private static class HashTableElement {
    private final long hashCode;
    private final boolean nativeKey;
    private final boolean nativeLongKey;
    private final boolean isNull;
    private final long longKey;
    private final byte[] keyBytes;
    private final int keyOffset, keyLength;
    private final byte[] valueBytes;
    private final int valueOffset, valueLength;

    public HashTableElement(long hashCode,
                            byte[] keyBytes, int keyOffset, int keyLength,
                            byte[] valueBytes, int valueOffset, int valueLength) {
      this.hashCode = hashCode;
      this.nativeKey = false;
      this.nativeLongKey = false;
      this.isNull = false;
      this.longKey = 0;

      this.keyBytes = keyBytes;
      this.keyOffset = keyOffset;
      this.keyLength = keyLength;

      this.valueBytes = valueBytes;
      this.valueOffset = valueOffset;
      this.valueLength = valueLength;
    }

    public HashTableElement(VectorMapJoinFastBatchDeserializer reader, boolean nativeLongKey) {
      // Capture immutable backing storage and primitive offsets. The reader and its
      // BytesWritable wrappers may be reused while this entry waits on a load queue.
      this.hashCode = reader.getHashCode();
      this.nativeKey = true;
      this.nativeLongKey = nativeLongKey;
      this.isNull = reader.isNull();
      this.longKey = nativeLongKey ? reader.getLongKey() : 0;
      this.keyBytes = reader.getBytes();
      this.keyOffset = reader.getKeyOffset();
      this.keyLength = reader.getKeyLength();
      this.valueBytes = reader.getBytes();
      this.valueOffset = reader.getValueOffset();
      this.valueLength = reader.getValueLength();
    }

    public void putRow(VectorMapJoinFastTableContainer tableContainer)
        throws IOException, HiveException, SerDeException {
      BytesWritable value = new BytesWritable(valueBytes, valueOffset, valueLength);
      if (!nativeKey) {
        tableContainer.putRow(hashCode, new BytesWritable(keyBytes, keyOffset, keyLength), value);
      } else if (nativeLongKey) {
        tableContainer.putRowNative(hashCode, longKey, isNull, value);
      } else {
        tableContainer.putRowNative(hashCode, keyBytes, keyOffset, keyLength, isNull, value);
      }
    }
  }

  private static class HashTableElementBatch {
    private static final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;
    private final HashTableElement[] batch;
    private int currentIndex;

    public HashTableElementBatch() {
      this.batch = new HashTableElement[BATCH_SIZE];
      this.currentIndex = 0;
    }

    public boolean addElement(HashTableElement h) {
      assert currentIndex < BATCH_SIZE;
      this.batch[this.currentIndex++] = h;
      return (this.currentIndex == BATCH_SIZE);
    }

    public HashTableElement getBatch(int i) {
      return this.batch[i];
    }

    public int getSize() {
      return this.currentIndex;
    }

    public void reset() {
      // In particular, release references to shared frame arrays after insertion.
      Arrays.fill(batch, 0, currentIndex, null);
      this.currentIndex = 0;
    }
  }
}
