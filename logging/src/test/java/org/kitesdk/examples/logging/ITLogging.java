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

import com.google.common.io.Resources;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.flume.Log4jAppender;
import org.kitesdk.examples.common.Cluster;

import static org.kitesdk.examples.common.TestUtil.run;
import static org.hamcrest.CoreMatchers.any;
import static org.junit.matchers.JUnitMatchers.containsString;

public class ITLogging {

  @Rule
  public static TemporaryFolder folder = new TemporaryFolder();

  private static Cluster cluster;

  @BeforeClass
  public static void startCluster() throws Exception {
    File flumeProperties = folder.newFile();
    FileUtils.copyURLToFile(Resources.getResource("flume.properties"), flumeProperties);
    cluster = new Cluster.Builder()
        .addHdfsService()
        .addFlumeAgent("tier1", flumeProperties)
        .build();
    cluster.start();
    Thread.sleep(5000L);
    configureLog4j();
  }

  @Before
  public void setUp() throws Exception {
    // delete dataset in case it already exists
    run(any(Integer.class), any(String.class), new DeleteDataset());
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    cluster.stop();
  }

  private static void configureLog4j() throws Exception {
    // configuration is done programmatically and not in log4j.properties so that so we
    // can defer initialization to after when the Flume Avro RPC source port is running
    Log4jAppender appender = new Log4jAppender();
    appender.setName("flume");
    appender.setHostname("localhost");
    appender.setPort(41415);
    appender.setDatasetRepositoryUri("repo:hdfs://localhost/tmp/data");
    appender.setDatasetName("events");
    appender.activateOptions();

    Logger.getLogger(org.kitesdk.examples.logging.App.class).addAppender(appender);
    Logger.getLogger(org.kitesdk.examples.logging.App.class).setLevel(Level.INFO);
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
