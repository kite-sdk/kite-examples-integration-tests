/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.examples.logging;

import java.io.File;
import org.apache.flume.node.Application;
import org.apache.flume.node.PropertiesFileConfigurationProvider;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.flume.Log4jAppender;
import org.kitesdk.examples.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kitesdk.examples.common.TestUtil.run;
import static org.hamcrest.CoreMatchers.any;
import static org.junit.matchers.JUnitMatchers.containsString;

public class ITLogging {

  private static final Logger logger = LoggerFactory
      .getLogger(ITLogging.class);


  private static Cluster cluster;
  private static Application flumeApplication;

  @BeforeClass
  public static void startCluster() throws Exception {
    cluster = new Cluster.Builder().addHdfsService().build();
    cluster.start();
    startFlume();
  }

  @Before
  public void setUp() throws Exception {
    // delete dataset in case it already exists
    run(any(Integer.class), any(String.class), new DeleteDataset());
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    stopFlume();
    cluster.stop();
  }

  private static void startFlume() throws Exception {

    logger.info("startFlume"); // this seems to be needed for it to work...

    String agentName = "tier1";
    File configurationFile = new File
        ("/Users/tom/workspace/kite-examples-integration-tests/logging/src/test/resources/flume.properties");
    PropertiesFileConfigurationProvider configurationProvider =
        new PropertiesFileConfigurationProvider(agentName,
            configurationFile);
    flumeApplication = new Application();
    flumeApplication.handleConfigurationEvent(configurationProvider.getConfiguration());

    flumeApplication.start();

    Thread.sleep(10000L);

    Log4jAppender appender = new Log4jAppender();
    appender.setName("flume");
    appender.setHostname("localhost");
    appender.setPort(41416);
    appender.setDatasetRepositoryUri("repo:hdfs://localhost/tmp/data");
    appender.setDatasetName("events");
    appender.activateOptions();

    org.apache.log4j.Logger.getLogger(org.kitesdk.examples.logging.App.class).addAppender(appender);
    org.apache.log4j.Logger.getLogger(org.kitesdk.examples.logging.App.class).setLevel
        (Level.INFO);

  }

  private static void stopFlume() {
    flumeApplication.stop();
  }

  @Test
  public void test() throws Exception {
    run(new CreateDataset());
    run(new App());
    Thread.sleep(40000); // wait for events to be flushed to HDFS
    run(containsString("{\"id\": 9, \"message\": \"Hello 9\"}"), new ReadDataset());
    run(new DeleteDataset());
  }

}
