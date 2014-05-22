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
package org.kitesdk.examples.data;

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import javax.servlet.ServletException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.flume.Log4jAppender;
import org.kitesdk.examples.common.Cluster;
import org.kitesdk.examples.common.TestUtil;
import org.kitesdk.examples.demo.LoggingServlet;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ITDemo {

  private static Cluster cluster;
  private static Configuration conf;

  @Rule
  public static TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startCluster() throws Exception {
    File flumeProperties = temp.newFile();
    FileUtils.copyURLToFile(
        Resources.getResource("flume.properties"), flumeProperties);

    cluster = new Cluster.Builder()
        .addHdfsService()
        .addHiveMetastoreService()
        .addFlumeAgent("tier1", flumeProperties)
        .build();
    cluster.start();
    conf = cluster.getConf();
  }

  @Before
  public void setUp() throws Exception {
    // delete datasets in case they already exist
    TestUtil.runDatasetCommand(conf, any(Integer.class),
        "delete events --directory /tmp/data");
    TestUtil.runDatasetCommand(conf, any(Integer.class),
        "delete sessions --directory /tmp/data");
    configureLog4j();
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    cluster.stop();
  }

  @Test
  public void testDatasetCreation() throws Exception {
    File standardEventAvsc = temp.newFile("standard_event.avsc");
    File sessionAvsc = temp.newFile("session.avsc");
    File partitionConfig = temp.newFile("year-month-day-hour-min.json");

    FileUtils.copyURLToFile(
        Resources.getResource("standard_event.avsc"), standardEventAvsc);
    FileUtils.copyURLToFile(
        Resources.getResource("session.avsc"), sessionAvsc);
    FileSystem fs = FileSystem.get(conf);

    Path schemas = new Path("/tmp/schemas");

    // hadoop fs -mkdir schemas
    fs.mkdirs(new Path("/tmp/schemas"));

    // hadoop fs -copyFromLocal demo-core/sr/main/avro/*.avsc
    fs.copyFromLocalFile(new Path(standardEventAvsc.toURI()), schemas);
    fs.copyFromLocalFile(new Path(sessionAvsc.toURI()), schemas);

    TestUtil.runDatasetCommand(conf, equalTo(0),
        "create sessions --schema hdfs:/tmp/schemas/session.avsc " +
            "--directory /tmp/data");

    TestUtil.runDatasetCommand(conf, equalTo(0),
        "partition-config --schema hdfs:/tmp/schemas/standard_event.avsc " +
            "timestamp:year timestamp:month timestamp:day timestamp:hour timestamp:minute " +
            "--output " + partitionConfig.toString());

    TestUtil.runDatasetCommand(conf, equalTo(0),
        "create events --schema hdfs:/tmp/schemas/standard_event.avsc " +
            "--partition-by " + partitionConfig.toString() + " " +
            "--directory /tmp/data");

    CallableServlet servlet = new CallableServlet();
    assertThat(servlet.get("Hello-1", "1"), equalTo(200));
    assertThat(servlet.get("Hello-2", "2"), equalTo(200));
    assertThat(servlet.get("Hello-3", "1"), equalTo(200));
    assertThat(servlet.get("Hello-4", "2"), equalTo(200));
    Thread.sleep(40000); // wait for events to be flushed to HDFS

    Dataset<Object> dataset = DatasetRepositories
        .open("repo:hive://localhost:9083/tmp/data").load("events");
    assertThat(count(dataset), equalTo(4));

    TestUtil.runDatasetCommand(conf, equalTo(0),
        "delete events --directory /tmp/data");

    TestUtil.runDatasetCommand(conf, equalTo(0),
        "delete sessions --directory /tmp/data");
  }

  private static class CallableServlet extends LoggingServlet {
    public int get(String message, String user) throws IOException, ServletException{
      MockHttpServletRequest request = new MockHttpServletRequest();
      request.addParameter("message", message);
      request.addParameter("user_id", user);
      MockHttpServletResponse response = new MockHttpServletResponse();
      doGet(request, response);
      return response.getStatus();
    }
  }

  public static <E> int count(Dataset<E> dataset) {
    DatasetReader<E> reader = dataset.newReader();
    int count = 0;
    try {
      reader.open();
      for (E item : reader) {
        count += 1;
      }
    } finally {
      reader.close();
    }
    return count;
  }

  private static void configureLog4j() throws Exception {
    // configuration is done programmatically and not in log4j.properties so that so we
    // can defer initialization to after the Flume Avro RPC source port is running
    Log4jAppender appender = new Log4jAppender();
    appender.setName("flume");
    appender.setHostname("localhost");
    appender.setPort(41415);
    appender.setDatasetRepositoryUri("repo:hive://localhost.localdomain:9083/tmp/data");
    appender.setDatasetName("events");
    appender.activateOptions();

    Logger.getLogger("org.kitesdk.examples.demo.LoggingServlet").addAppender(appender);
    Logger.getLogger("org.kitesdk.examples.demo.LoggingServlet").setLevel(Level.INFO);
  }

}
