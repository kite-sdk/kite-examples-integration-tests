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
package org.kitesdk.examples.common;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.hamcrest.Matcher;
import org.slf4j.Logger;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtil {

  public static void run(Tool tool, String... args) throws Exception {
    run(equalTo(0), any(String.class), tool, args);
  }

  public static void run(Matcher<String> stdOutMatcher, Tool tool,
      String... args) throws Exception {
    run(equalTo(0), stdOutMatcher, tool, args);
  }

  public static void run(Matcher<Integer> exitCodeMatcher,
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

  public static void runDatasetCommand(Configuration conf,
                                       Matcher<Integer> exitCodeMatcher,
                                       String arguments) throws Exception {
    int rc = org.kitesdk.cli.TestUtil.run(conf, arguments.split(" "));
    assertThat(rc, exitCodeMatcher);
  }
}
