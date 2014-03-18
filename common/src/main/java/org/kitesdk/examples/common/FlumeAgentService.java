package org.kitesdk.examples.common;

import java.io.File;
import java.io.IOException;
import org.apache.flume.node.Application;
import org.apache.flume.node.PropertiesFileConfigurationProvider;

class FlumeAgentService implements Cluster.Service {

  private PropertiesFileConfigurationProvider configurationProvider;
  private Application flumeApplication;

  public FlumeAgentService(String agentName, File configurationFile) {
    configurationProvider = new PropertiesFileConfigurationProvider(agentName,
            configurationFile);
    flumeApplication = new Application();
  }

  @Override
  public void start() throws IOException {
    flumeApplication.handleConfigurationEvent(configurationProvider.getConfiguration());
    flumeApplication.start();
  }

  @Override
  public void stop() throws IOException {
    flumeApplication.stop();
  }
}
