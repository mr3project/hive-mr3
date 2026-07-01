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
package org.apache.hadoop.hive.llap.cache;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hive.common.util.CleanerUtil;

public final class SimpleAllocator implements Allocator, BuddyAllocatorMXBean {
  private final boolean isDirect;

  public SimpleAllocator(Configuration conf) {
    isDirect = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT);
    if (LlapIoImpl.LOG.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Simple allocator with " + (isDirect ? "direct" : "byte") + " buffers");
    }
  }

  @Override
  @Deprecated
  public void allocateMultiple(MemoryBuffer[] dest, int size) {
    allocateMultiple(dest, size, null);
  }

  /* All-or-nothing semantics:
   * If ByteBuffer allocation fails, cleanupByteBuffers(bbs) cleans up and dest[] remains untouched.
   * Only after all allocations succeed does it update dest[].
   */
  // MemoryBuffer in dest[] is uninitialized, i.e., with no backing ByteBuffer.
  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory) {
    int len = dest.length;

    // Pre-allocate the ByteBuffers for those slots
    ByteBuffer[] bbs = new ByteBuffer[len];
    try {
      for (int k = 0; k < len; k++) {
        bbs[k] = isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
      }
    } catch (OutOfMemoryError | RuntimeException e) {
      cleanupByteBuffers(bbs); // frees only the ones actually created
      throw e;
    }

    // Success → initialize and publish
    for (int k = 0; k < len; k++) {
      LlapAllocatorBuffer buf = (LlapAllocatorBuffer)dest[k];
      buf.initialize(bbs[k], 0, size);
    }
  }

  /**
   * Helper method to cleanup partially allocated ByteBuffers
   */
  private void cleanupByteBuffers(ByteBuffer[] byteBuffers) {
    for (int i = 0; i < byteBuffers.length; ++i) {
      ByteBuffer bb = byteBuffers[i];
      if (bb == null) continue; // This and subsequent buffers were not allocated

      // Only cleanup direct buffers as heap buffers will be GC'd
      if (bb.isDirect() && CleanerUtil.UNMAP_SUPPORTED) {
        try {
          CleanerUtil.getCleaner().freeBuffer(bb);
        } catch (Throwable t) {
          LlapIoImpl.LOG.warn("Error cleaning up DirectByteBuffer during allocation failure", t);
        }
      }
      byteBuffers[i] = null; // Help GC
    }
  }

  @Override
  public void deallocate(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    ByteBuffer bb = buf.byteBuffer;
    buf.byteBuffer = null;
    if (!bb.isDirect()) return;
    if (!CleanerUtil.UNMAP_SUPPORTED) {
      return;
    }
    try {
      CleanerUtil.getCleaner().freeBuffer(bb);
    } catch (Throwable t) {
      LlapIoImpl.LOG.warn("Error using DirectByteBuffer cleaner", t);
    }
  }

  @Override
  public boolean isDirectAlloc() {
    return isDirect;
  }

  @Override
  @Deprecated
  public LlapAllocatorBuffer createUnallocated() {
    return new LlapDataBuffer();
  }

  // BuddyAllocatorMXBean
  @Override
  public boolean getIsDirect() {
    return isDirect;
  }

  @Override
  public int getMinAllocation() {
    return 0;
  }

  @Override
  public int getMaxAllocation() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getArenaSize() {
    return -1;
  }

  @Override
  public long getMaxCacheSize() {
    return Integer.MAX_VALUE;
  }
}
