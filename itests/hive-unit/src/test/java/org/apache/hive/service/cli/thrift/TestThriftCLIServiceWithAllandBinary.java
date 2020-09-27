package org.apache.hive.service.cli.thrift;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * TestThriftHttpCLIService.
 * This tests ThriftCLIService started in http mode.
 */

public class TestThriftCLIServiceWithAllandBinary extends ThriftCLIServiceTest {

  private static String transportMode = "all";

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set up the base class
    ThriftCLIServiceTest.setUpBeforeClass();

    assertNotNull(port);
    assertNotNull(hiveServer2);
    assertNotNull(hiveConf);

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NONE.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);

    startHiveServer2WithConf(hiveConf);

    client = getHttpServiceClientInternal();
  }

  /**
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ThriftCLIServiceTest.tearDownAfterClass();
  }
  static ThriftCLIServiceClient getHttpServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        continue;
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }
}
