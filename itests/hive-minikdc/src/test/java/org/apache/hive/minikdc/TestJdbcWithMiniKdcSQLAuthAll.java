package org.apache.hive.minikdc;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.BeforeClass;

public class TestJdbcWithMiniKdcSQLAuthAll extends JdbcWithMiniKdcSQLAuthTest {

  @BeforeClass
  public static void beforeTest() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, MiniHS2.HS2_ALL_MODE);
    JdbcWithMiniKdcSQLAuthTest.beforeTestBase();
  }
}
