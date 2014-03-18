package org.kitesdk.examples.common;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {

  private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

  private static final String USE_EXTERNAL_CLUSTER_PROPERTY_NAME = "useExternalCluster";

  public static boolean isUsingExternalCluster() {
    return Boolean.getBoolean(USE_EXTERNAL_CLUSTER_PROPERTY_NAME);
  }

  private List<Service> services;

  public Cluster(List<Service> services) {
    this.services = services;
  }

  public void start() throws IOException {
    if (isUsingExternalCluster()) {
      logger.info("Not starting in-VM cluster: using external cluster");
      return;
    } else {
      logger.info("Starting in-VM cluster");
    }
    for (Service service : services) {
      service.start();
    }
  }

  public void stop() throws IOException {
    if (isUsingExternalCluster()) {
      logger.info("Not stopping in-VM cluster: using external cluster");
      return;
    } else {
      logger.info("Stopping in-VM cluster");
    }
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

    public Builder addHiveMetastoreService() {
      services.add(new HiveMetastoreService());
      return this;
    }

    public Builder addFlumeAgent(String agentName, File configurationFile) {
      services.add(new FlumeAgentService(agentName, configurationFile));
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
