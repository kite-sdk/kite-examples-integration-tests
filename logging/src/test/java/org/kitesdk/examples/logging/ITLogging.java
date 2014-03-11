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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flume.Context;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.flume.conf.FlumeConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.kitesdk.examples.common.TestUtil.run;
import static org.hamcrest.CoreMatchers.any;
import static org.junit.matchers.JUnitMatchers.containsString;

public class ITLogging {

  private EmbeddedAgent flumeAgent;

  @Before
  public void setUp() throws Exception {
    // delete dataset in case it already exists
    run(any(Integer.class), any(String.class), new DeleteDataset());

    startFlume();
  }

  @After
  public void tearDown() throws Exception {
    stopFlume();
  }

  private void startFlume() throws IOException {
    flumeAgent = new EmbeddedAgent("tier1");
    Properties properties = new Properties();
    properties.load(Resources.getResource("flume.properties").openStream());
    properties.setProperty("tier1.sinks.sink-1.hdfs.proxyUser",
        System.getProperty("user.name"));

    FlumeConfiguration conf = new FlumeConfiguration(properties);
    FlumeConfiguration.AgentConfiguration agentConfiguration = conf.getConfigurationFor("tier1");
    Map<String, String> embeddedAgentConfiguration = Maps.newHashMap();

    Context channelContext = Iterables.getOnlyElement(agentConfiguration.getChannelContext().entrySet()).getValue();
    for (Map.Entry<String, String> e : channelContext.getParameters().entrySet()) {
      embeddedAgentConfiguration.put("channel." + e.getKey(), e.getValue());
    }
    Context sourceContext = Iterables.getOnlyElement(agentConfiguration
        .getSourceContext().entrySet()).getValue();
    for (Map.Entry<String, String> e : sourceContext.getParameters().entrySet()) {
      embeddedAgentConfiguration.put("source." + e.getKey(), e.getValue());
    }
    Context sinkContext = Iterables.getOnlyElement(agentConfiguration.getSinkContext()
        .entrySet()).getValue();
    for (Map.Entry<String, String> e : sinkContext.getParameters().entrySet()) {
      embeddedAgentConfiguration.put("sink." + e.getKey(), e.getValue());
    }
    System.out.println(agentConfiguration.getChannelContext());
    System.out.println(embeddedAgentConfiguration);

    flumeAgent.configure(embeddedAgentConfiguration);
    flumeAgent.start();
  }

  private void stopFlume() {
    flumeAgent.stop();
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
