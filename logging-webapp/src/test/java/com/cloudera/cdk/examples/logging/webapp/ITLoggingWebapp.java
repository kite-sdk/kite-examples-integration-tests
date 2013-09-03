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
package com.cloudera.cdk.examples.logging.webapp;

import com.cloudera.cdk.examples.logging.DropDataset;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.startup.Tomcat;
import org.apache.hadoop.util.Tool;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ITLoggingWebapp {
  @Before
  public void setUp() throws Exception {
    // drop dataset in case it already exists
    run(any(Integer.class), any(String.class), new DropDataset());

    startTomcat();
  }

  @Test
  public void test() throws Exception {

  }

  private void startTomcat() throws Exception {
    String appBase = "target/wars/logging-webapp.war";
    Tomcat tomcat = new Tomcat();
    tomcat.setPort(8080);

    tomcat.setBaseDir(".");
    tomcat.getHost().setAppBase(".");

    String contextPath = "/logging-webapp";

    // Add AprLifecycleListener
    StandardServer server = (StandardServer)tomcat.getServer();
    AprLifecycleListener listener = new AprLifecycleListener();
    server.addLifecycleListener(listener);

    tomcat.addWebapp(contextPath, appBase);
    tomcat.start();
    tomcat.getServer().await();
  }

  private static void run(Tool tool, String... args) throws Exception {
    run(equalTo(0), any(String.class), tool, args);
  }

  private static void run(Matcher<String> stdOutMatcher, Tool tool,
      String... args) throws Exception {
    run(equalTo(0), stdOutMatcher, tool, args);
  }

  private static void run(Matcher<Integer> exitCodeMatcher,
      Matcher<String> stdOutMatcher,
      Tool tool,
      String... args) throws Exception {
    PrintStream oldStdOut = System.out;
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      int rc = tool.run(args);
      assertThat(rc, exitCodeMatcher);
      assertThat(out.toString(), stdOutMatcher);
    } finally {
      System.setOut(oldStdOut);
    }
  }
}
