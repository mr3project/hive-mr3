package org.apache.hadoop.hive.llap.io.session.heap;

import java.nio.ByteBuffer;
import org.junit.Test;
import static org.junit.Assert.*;

public class OnHeapLowLevelCacheTest {
  @Test
  public void testPutGetEvict() {
    OnHeapLowLevelCache cache = new OnHeapLowLevelCache(10);
    ByteBuffer b1 = ByteBuffer.wrap(new byte[8]);
    cache.put("a", b1);
    assertNotNull(cache.get("a"));
    ByteBuffer b2 = ByteBuffer.wrap(new byte[8]);
    cache.put("b", b2); // should evict one due to capacity
    int count = 0;
    if (cache.get("a") != null) count++;
    if (cache.get("b") != null) count++;
    assertEquals(1, count);
  }
}
