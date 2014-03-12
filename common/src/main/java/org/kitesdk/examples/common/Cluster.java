package org.kitesdk.examples.common;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class Cluster {

  private List<Service> services;

  public Cluster(List<Service> services) {
    this.services = services;
  }

  public void start() throws IOException {
    for (Service service : services) {
      service.start();
    }
  }

  public void stop() throws IOException {
    for (Service service : services) {
      service.stop();
    }
  }

  public static class Builder {

    private List<Service> services = Lists.newArrayList();

    public Builder() {
    }

    public Builder addHdfsService() {
      services.add(new HdfsService());
      return this;
    }

    public Builder addFlumeAgent(File configurationFile) {
      services.add(new FlumeAgentService(configurationFile));
      return this;
    }

    public Cluster build() {
      return new Cluster(services);
    }

  }

  static interface Service {
    void start() throws IOException;
    void stop() throws IOException;
  }

}
