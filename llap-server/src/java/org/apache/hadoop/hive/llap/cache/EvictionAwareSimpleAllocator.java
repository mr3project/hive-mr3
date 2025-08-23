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

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

import java.util.concurrent.atomic.AtomicBoolean;

// A heap-based allocator that is aware of the eviction framework.
public final class EvictionAwareSimpleAllocator implements EvictionAwareStoppableAllocator {

  private final SimpleAllocator delegate;
  private final MemoryManager memoryManager;
  private final int allocatorMaxAlloc;  // corresponds to LLAP_ALLOCATOR_MAX_ALLOC

  public EvictionAwareSimpleAllocator(
      SimpleAllocator delegate, MemoryManager memoryManager, int allocatorMaxAlloc) {
    this.delegate = delegate;
    this.memoryManager = memoryManager;
    this.allocatorMaxAlloc = allocatorMaxAlloc;
  }

  // MemoryBuffer in dest[] is either null or uninitialized (i.e., with no backing ByteBuffer).
  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size,
                               BufferObjectFactory factory,
                               AtomicBoolean isStopped) throws AllocatorOutOfMemoryException {
    final long reserved = (long) size * dest.length;
    memoryManager.reserveMemory(reserved, isStopped);
    initMemoryBuffers(dest, factory);

    try {
      // All-or-nothing guarantee is enforced by SimpleAllocator.
      delegate.allocateMultiple(dest, size, factory);
    } catch (Throwable t) {
      // We reserved; delegate didn’t deliver → release everything and rethrow.
      memoryManager.releaseMemory(reserved);
      throw new AllocatorOutOfMemoryException("Memory allocation fails, size=" + reserved);
    }
  }

  private void initMemoryBuffers(MemoryBuffer[] dest, BufferObjectFactory factory) {
    for (int i = 0; i < dest.length; ++i) {
      if (dest[i] != null) continue;
      dest[i] = factory != null ? factory.create() : new LlapDataBuffer();
    }
  }

  // Deallocate for proactive/instant eviction
  @Override
  public void deallocateProactivelyEvicted(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    assert buf.isInvalid();
    delegate.deallocate(buf);
    memoryManager.releaseMemory(buf.getMemoryUsage());
  }

  // Deallocate for reactive eviction (from memory manager)
  @Override
  public void deallocateEvicted(MemoryBuffer buffer) {
    // For reactive eviction, memory is not released back to the manager,
    // as it's assumed to be immediately re-used by the pending allocation.
    delegate.deallocate(buffer);
  }

  // Deallocate for normal buffer release (e.g., uncached buffer)
  @Override
  public void deallocate(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    memoryManager.releaseMemory(buf.getMemoryUsage());
    delegate.deallocate(buffer);
  }

  @Override
  public boolean isDirectAlloc() {
    return delegate.isDirectAlloc();
  }

  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory) {
    allocateMultiple(dest, size, factory, null);
  }

  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size) {
    allocateMultiple(dest, size, null, null);
  }

  @Override
  public MemoryBuffer createUnallocated() {
    return new LlapDataBuffer();
  }

  @Override
  public int getMaxAllocation() {
    return allocatorMaxAlloc;
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    sb.append("\nAllocator memory state: ");
    long currentFreeMemory = Runtime.getRuntime().freeMemory();
    sb.append("\nCurrent free memory = ").append(currentFreeMemory);
    sb.append("\n");
  }
}