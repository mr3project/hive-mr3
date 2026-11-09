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

package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestObjectCache {
  @Rule
  public final Timeout timeout = Timeout.seconds(15);

  private ObservedRegistry registry;
  private ObjectCache cache;

  @Before
  public void setUp() {
    ObjectCache.clearObjectRegistry();
    registry = new ObservedRegistry();
    ProcessorContext context = mock(ProcessorContext.class);
    when(context.getObjectRegistry()).thenReturn(registry);
    ObjectCache.setupObjectRegistry(context);
    cache = new ObjectCache();
  }

  @After
  public void tearDown() {
    ObjectCache.clearObjectRegistry();
  }

  @Test
  public void testSameKeyLoadsOnceAcrossCacheInstances() throws Exception {
    ObjectCache otherCache = new ObjectCache();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();
    Object value = new Object();
    Future<Object> first = cache.retrieveAsync("shared", () -> {
      loads.incrementAndGet();
      started.countDown();
      await(release);
      return value;
    });
    try {
      await(started);
      CountDownLatch secondMiss = registry.observeNextMiss();
      Future<Object> second = otherCache.retrieveAsync("shared", () -> {
        loads.incrementAndGet();
        return new Object();
      });
      // Ensure the second request observes the miss while the first is still loading.
      await(secondMiss);
      release.countDown();
      assertSame(value, first.get(5, TimeUnit.SECONDS));
      assertSame(value, second.get(5, TimeUnit.SECONDS));
      assertEquals(1, loads.get());
    } finally {
      release.countDown();
      first.cancel(true);
    }
  }

  @Test
  public void testDifferentKeysLoadConcurrentlyAndLookupDoesNotWait() throws Exception {
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> first = cache.retrieveAsync("first", () -> {
      bothStarted.countDown();
      await(release);
      return "one";
    });
    Future<String> second = cache.retrieveAsync("second", () -> {
      bothStarted.countDown();
      await(release);
      return "two";
    });
    try {
      await(bothStarted);
      assertFalse(first.isDone());
      assertFalse(second.isDone());
      assertNull(cache.retrieve("first"));
      assertNull(cache.retrieve("second"));
      release.countDown();
      assertEquals("one", first.get(5, TimeUnit.SECONDS));
      assertEquals("two", second.get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      first.cancel(true);
      second.cancel(true);
    }
  }

  @Test
  public void testFailedLoadCanBeRetried() throws Exception {
    IllegalStateException failure = new IllegalStateException("load failed");
    try {
      cache.retrieve("retry", () -> { throw failure; });
      fail("Expected load failure");
    } catch (HiveException e) {
      assertSame(failure, e.getCause());
    }
    assertNull(cache.retrieve("retry"));
    Object value = new Object();
    assertSame(value, cache.retrieve("retry", () -> value));
    assertSame(value, new ObjectCache().retrieve("retry", () -> {
      throw new AssertionError("Successful load should be cached");
    }));
  }

  @Test
  public void testLoadErrorIsPreservedAndCanBeRetried() throws Exception {
    AssertionError failure = new AssertionError("load failed");
    try {
      cache.retrieve("retry", () -> { throw failure; });
      fail("Expected load error");
    } catch (AssertionError e) {
      assertSame(failure, e);
    }
    assertEquals("loaded", cache.retrieve("retry", () -> "loaded"));
  }

  @Test
  public void testInterruptedWaiterDoesNotCancelOwner() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> owner = cache.retrieveAsync("shared", () -> {
      started.countDown();
      await(release);
      return "loaded";
    });
    AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
    AtomicBoolean interrupted = new AtomicBoolean();
    CountDownLatch waiterFinished = new CountDownLatch(1);
    Thread waiter = new Thread(() -> {
      try {
        cache.retrieve("shared", () -> {
          throw new AssertionError("Waiter must reuse the owner's load");
        });
        waiterFailure.set(new AssertionError("Expected interrupted wait"));
      } catch (HiveException e) {
        if (!(e.getCause() instanceof InterruptedException)) {
          waiterFailure.set(e);
        }
        interrupted.set(Thread.currentThread().isInterrupted());
      } catch (Throwable t) {
        waiterFailure.set(t);
      } finally {
        waiterFinished.countDown();
      }
    });
    try {
      await(started);
      CountDownLatch waiterMiss = registry.observeNextMiss();
      waiter.start();
      await(waiterMiss);
      waiter.interrupt();
      await(waiterFinished);
      assertNull(waiterFailure.get());
      assertTrue(interrupted.get());
      assertFalse(owner.isDone());

      CountDownLatch nextMiss = registry.observeNextMiss();
      Future<String> next = cache.retrieveAsync("shared", () -> {
        throw new AssertionError("Interrupted waiter must not remove the owner's load");
      });
      await(nextMiss);
      release.countDown();
      assertEquals("loaded", owner.get(5, TimeUnit.SECONDS));
      assertEquals("loaded", next.get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      owner.cancel(true);
      waiter.interrupt();
      if (waiter.getState() != Thread.State.NEW) {
        waiter.join(5000);
      }
    }
  }

  @Test
  public void testSameKeyInDifferentRegistriesLoadsIndependently() throws Exception {
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    Future<String> first = cache.retrieveAsync("key", () -> {
      bothStarted.countDown();
      await(release);
      return "first registry";
    });
    ObjectCache.setupObjectRegistryDummy();
    ObjectCache otherCache = new ObjectCache();
    Future<String> second = otherCache.retrieveAsync("key", () -> {
      bothStarted.countDown();
      await(release);
      return "second registry";
    });
    try {
      await(bothStarted);
      release.countDown();
      assertEquals("first registry", first.get(5, TimeUnit.SECONDS));
      assertEquals("second registry", second.get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      first.cancel(true);
      second.cancel(true);
    }
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertTrue("Timed out waiting for concurrent request", latch.await(5, TimeUnit.SECONDS));
  }

  private static final class ObservedRegistry extends ObjectRegistryImpl {
    private volatile CountDownLatch miss;

    CountDownLatch observeNextMiss() {
      CountDownLatch next = new CountDownLatch(1);
      miss = next;
      return next;
    }

    @Override
    public Object get(String key) {
      Object value = super.get(key);
      CountDownLatch current = miss;
      if (value == null && current != null) {
        current.countDown();
      }
      return value;
    }
  }
}
