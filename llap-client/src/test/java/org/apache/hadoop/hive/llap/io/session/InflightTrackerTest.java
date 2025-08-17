package org.apache.hadoop.hive.llap.io.session;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import static org.junit.Assert.*;

public class InflightTrackerTest {
  @Test
  public void testDeduplication() throws Exception {
    InflightTracker tracker = new InflightTracker();
    AtomicInteger loads = new AtomicInteger();
    Callable<Integer> call = () -> tracker.getOrLoad("k", () -> {
      loads.incrementAndGet();
      try { Thread.sleep(50); } catch (InterruptedException e) {}
      return 5;
    });
    var ex = Executors.newFixedThreadPool(2);
    Future<Integer> f1 = ex.submit(call);
    Future<Integer> f2 = ex.submit(call);
    assertEquals(5, (int)f1.get());
    assertEquals(5, (int)f2.get());
    assertEquals(1, loads.get());
    ex.shutdown();
  }
}
