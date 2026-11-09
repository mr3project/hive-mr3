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
package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestOperatorInitialization {
  private final Configuration conf = new Configuration(false);
  private final CompilationOpContext context = new CompilationOpContext();
  private final List<String> completions = new ArrayList<>();

  @Test
  public void testDummyParentsStartBothLoadsBeforeWaiting() throws Exception {
    ExecutorService loaders = Executors.newFixedThreadPool(2);
    ExecutorService initializer = Executors.newSingleThreadExecutor();
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch releaseSecond = new CountDownLatch(1);
    try {
      TestOperator main = operator("main");
      TestOperator dummyFirst = operator("dummyFirst");
      TestOperator dummySecond = operator("dummySecond");
      TestOperator first = operator("first");
      TestOperator second = operator("second");
      first.operation = () -> loaders.submit(() -> load(bothStarted, releaseFirst, "one"));
      second.operation = () -> loaders.submit(() -> load(bothStarted, releaseSecond, "two"));
      connect(main, first);
      connect(dummyFirst, first);
      connect(first, second);
      connect(dummySecond, second);

      main.initialize(conf, null);
      assertEquals(0, first.initializations);
      assertEquals(0, second.initializations);

      Future<?> initialized = initializer.submit(() -> {
        initializeBatch(Arrays.asList(dummyFirst, dummySecond));
        return null;
      });
      assertTrue("Both hash-table loads must start before either is released",
          bothStarted.await(10, TimeUnit.SECONDS));
      assertFalse(initialized.isDone());
      releaseFirst.countDown();
      assertFalse("Initialization must also wait for the second load", initialized.isDone());
      releaseSecond.countDown();
      initialized.get(10, TimeUnit.SECONDS);

      assertArrayEquals(new Object[] {"one"}, first.results);
      assertArrayEquals(new Object[] {"two"}, second.results);
      assertArrayEquals(new Object[0], dummyFirst.results);
      assertArrayEquals(new Object[0], dummySecond.results);
      assertCompletions("main", "first", "dummyFirst", "second", "dummySecond");
      for (TestOperator op : Arrays.asList(main, dummyFirst, dummySecond, first, second)) {
        assertEquals(1, op.initializations);
        assertEquals(1, op.completeCalls);
      }
    } finally {
      releaseFirst.countDown();
      releaseSecond.countDown();
      shutdown(initializer);
      shutdown(loaders);
    }
  }

  @Test
  public void testSharedDescendantsAndRepeatedRootsCompleteOnce() throws Exception {
    TestOperator first = operator("first");
    TestOperator second = operator("second");
    TestOperator shared = operator("shared");
    shared.operation = () -> CompletableFuture.completedFuture("shared result");
    connect(first, shared);
    connect(second, shared);

    initializeBatch(Arrays.asList(first, second, first));

    for (TestOperator op : Arrays.asList(first, second, shared)) {
      assertEquals(1, op.initializations);
      assertEquals(1, op.completeCalls);
    }
    assertArrayEquals(new Object[] {"shared result"}, shared.results);
    assertTrue(shared.asyncInitOperations.isEmpty());
    assertCompletions("first", "shared", "second");
  }

  @Test
  public void testEachOperatorCompletesWithItsOwnResults() throws Exception {
    TestOperator root = operator("root");
    TestOperator child = operator("child");
    TestOperator leaf = operator("leaf");
    root.operation = () -> CompletableFuture.completedFuture("root result");
    child.operation = () -> CompletableFuture.completedFuture("child result");
    connect(root, child);
    connect(child, leaf);

    initializeBatch(singletonList(root));

    assertCompletions("leaf", "child", "root");
    assertArrayEquals(new Object[0], leaf.results);
    assertArrayEquals(new Object[] {"child result"}, child.results);
    assertArrayEquals(new Object[] {"root result"}, root.results);
  }

  @Test
  public void testSeparateRootsDeferCompletionUntilSecondPass() throws Exception {
    TestOperator first = operator("first");
    TestOperator second = operator("second");
    first.operation = () -> CompletableFuture.completedFuture("first result");

    first.initializeDeferringAsyncWaits(conf, null);
    second.initializeDeferringAsyncWaits(conf, null);
    assertEquals(0, first.completeCalls);
    assertFalse(first.asyncInitOperations.isEmpty());
    // Operators without futures complete synchronously during the first pass.
    assertEquals(1, second.completeCalls);

    // Ordinary initialization remains synchronous even between the two passes.
    assertOrdinaryInitializationCompletes();
    first.completeDeferredInitialization();
    second.completeDeferredInitialization();
    first.completeDeferredInitialization();

    assertCompletions("ordinary", "first", "second");
    assertEquals(1, first.completeCalls);
    assertEquals(1, second.completeCalls);
    assertArrayEquals(new Object[] {"first result"}, first.results);
    assertTrue(first.asyncInitOperations.isEmpty());
  }

  @Test
  public void testCompletionCallbackCanInitializeOrdinaryOperator() throws Exception {
    TestOperator first = operator("first");
    first.completionAction = this::assertOrdinaryInitializationCompletes;

    initializeBatch(singletonList(first));

    assertCompletions("ordinary", "first");
    assertEquals(1, first.completeCalls);
  }

  @Test
  public void testSynchronousFailureCancelsEarlierAndPartialLoads() throws Exception {
    TestOperator first = operator("first");
    TestOperator failing = operator("failing");
    CompletableFuture<String> firstLoad = new CompletableFuture<>();
    CompletableFuture<String> failingLoad = new CompletableFuture<>();
    first.operation = () -> firstLoad;
    failing.operation = () -> failingLoad;
    failing.initializationFailure = new HiveException("initialization failed");

    HiveException failure = assertThrows(HiveException.class,
        () -> initializeBatch(Arrays.asList(first, failing)));

    assertSame(failing.initializationFailure, failure);
    assertTrue(firstLoad.isCancelled());
    assertTrue(failingLoad.isCancelled());
    assertTrue(completions.isEmpty());
    assertOrdinaryInitializationCompletes();
  }

  @Test
  public void testFailedFutureCancelsSiblingLoads() throws Exception {
    TestOperator first = operator("first");
    TestOperator second = operator("second");
    CompletableFuture<String> firstLoad = new CompletableFuture<>();
    CompletableFuture<String> secondLoad = new CompletableFuture<>();
    Exception loadFailure = new Exception("load failed");
    firstLoad.completeExceptionally(loadFailure);
    first.operation = () -> firstLoad;
    second.operation = () -> secondLoad;

    HiveException failure = assertThrows(HiveException.class,
        () -> initializeBatch(Arrays.asList(first, second)));

    assertSame(loadFailure, failure.getCause());
    assertEquals(1, second.initializations);
    assertTrue(secondLoad.isCancelled());
    assertTrue(completions.isEmpty());
    assertOrdinaryInitializationCompletes();
  }

  @Test
  public void testCompletionFailureCancelsSiblingLoads() throws Exception {
    TestOperator first = operator("first");
    TestOperator second = operator("second");
    CompletableFuture<String> secondLoad = new CompletableFuture<>();
    first.operation = () -> CompletableFuture.completedFuture("loaded");
    second.operation = () -> secondLoad;
    first.completionFailure = new HiveException("completion failed");

    HiveException failure = assertThrows(HiveException.class,
        () -> initializeBatch(Arrays.asList(first, second)));

    assertSame(first.completionFailure, failure);
    assertTrue(secondLoad.isCancelled());
    assertEquals(0, second.completeCalls);
    assertOrdinaryInitializationCompletes();
  }

  @Test
  public void testAbortWhileWaitingCancelsSiblingLoads() throws Exception {
    assertWaitingFailureCancelsSiblings(false);
  }

  @Test
  public void testInterruptWhileWaitingCancelsSiblingLoads() throws Exception {
    assertWaitingFailureCancelsSiblings(true);
  }

  private void assertWaitingFailureCancelsSiblings(boolean interrupt) throws Exception {
    ExecutorService initializer = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> initializerThread = new AtomicReference<>();
    WaitingFuture firstLoad = new WaitingFuture();
    CompletableFuture<String> secondLoad = new CompletableFuture<>();
    TestOperator first = operator("first");
    TestOperator second = operator("second");
    first.operation = () -> firstLoad;
    second.operation = () -> secondLoad;
    try {
      Future<?> initialized = initializer.submit(() -> {
        initializerThread.set(Thread.currentThread());
        initializeBatch(Arrays.asList(first, second));
        return null;
      });
      assertTrue(firstLoad.waiting.await(10, TimeUnit.SECONDS));
      if (interrupt) {
        initializerThread.get().interrupt();
      } else {
        first.abort();
      }
      ExecutionException failure = assertThrows(ExecutionException.class,
          () -> initialized.get(10, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof HiveException);
      if (interrupt) {
        assertTrue(failure.getCause().getCause() instanceof InterruptedException);
      }
      assertTrue(firstLoad.isCancelled());
      assertTrue(secondLoad.isCancelled());
      assertTrue(completions.isEmpty());
      // Ordinary initialization must still complete on the same processor thread.
      initializer.submit(() -> {
        assertOrdinaryInitializationCompletes();
        return null;
      }).get(10, TimeUnit.SECONDS);
    } finally {
      firstLoad.cancel(true);
      secondLoad.cancel(true);
      shutdown(initializer);
    }
  }

  private void initializeBatch(List<? extends Operator<?>> roots) throws HiveException {
    try {
      for (Operator<?> root : roots) {
        root.initializeDeferringAsyncWaits(conf, null);
      }
      for (Operator<?> root : roots) {
        root.completeDeferredInitialization();
      }
    } catch (Throwable e) {
      for (Operator<?> root : roots) {
        root.cancelDeferredInitialization();
      }
      throw e;
    }
  }

  private void assertOrdinaryInitializationCompletes() throws HiveException {
    TestOperator ordinary = operator("ordinary");
    ordinary.operation = () -> CompletableFuture.completedFuture("ordinary result");
    ordinary.initialize(conf, null);
    assertEquals(1, ordinary.completeCalls);
    assertArrayEquals(new Object[] {"ordinary result"}, ordinary.results);
    assertTrue(ordinary.asyncInitOperations.isEmpty());
  }

  private void assertCompletions(String... expected) {
    assertEquals(expected.length, completions.size());
    assertEquals(new HashSet<>(Arrays.asList(expected)), new HashSet<>(completions));
  }

  private TestOperator operator(String name) {
    return new TestOperator(context, name, completions);
  }

  private static void connect(TestOperator parent, TestOperator child) {
    parent.getChildOperators().add(child);
    child.getParentOperators().add(parent);
  }

  private static String load(CountDownLatch started, CountDownLatch release, String result)
      throws InterruptedException {
    started.countDown();
    assertTrue("Load was not released", release.await(10, TimeUnit.SECONDS));
    return result;
  }

  private static void shutdown(ExecutorService executor) throws InterruptedException {
    executor.shutdownNow();
    assertTrue("Executor did not stop", executor.awaitTermination(10, TimeUnit.SECONDS));
  }

  private static final class WaitingFuture extends CompletableFuture<String> {
    private final CountDownLatch waiting = new CountDownLatch(1);

    @Override
    public String get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      waiting.countDown();
      return super.get(timeout, unit);
    }
  }

  @FunctionalInterface
  private interface InitializationAction {
    void run() throws HiveException;
  }

  private static final class TestOperator extends Operator<OperatorDesc> {
    private final String label;
    private final List<String> completions;
    private Supplier<Future<?>> operation;
    private InitializationAction initializationAction;
    private InitializationAction completionAction;
    private HiveException initializationFailure;
    private HiveException completionFailure;
    private int initializations;
    private int completeCalls;
    private Object[] results;

    private TestOperator(CompilationOpContext context, String label, List<String> completions) {
      super(context);
      this.label = label;
      this.completions = completions;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
      super.initializeOp(hconf);
      initializations++;
      if (operation != null) {
        asyncInitOperations.add(operation.get());
      }
      if (initializationAction != null) {
        initializationAction.run();
      }
      if (initializationFailure != null) {
        throw initializationFailure;
      }
    }

    @Override
    protected void completeInitializationOp(Object[] os) throws HiveException {
      completeCalls++;
      results = os;
      if (completionFailure != null) {
        throw completionFailure;
      }
      if (completionAction != null) {
        completionAction.run();
      }
      completions.add(label);
    }

    @Override
    public void process(Object row, int tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
      return "TEST";
    }

    @Override
    public OperatorType getType() {
      return null;
    }
  }
}
