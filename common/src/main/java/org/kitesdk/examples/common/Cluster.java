package org.kitesdk.examples.common;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {

  private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

  private static final String USE_EXTERNAL_CLUSTER_PROPERTY_NAME = "useExternalCluster";
  private static TemporaryFolder temp = new TemporaryFolder();
  private static File clusterConfigFile = null;

  public static boolean isUsingExternalCluster() {
    return Boolean.getBoolean(USE_EXTERNAL_CLUSTER_PROPERTY_NAME);
  }

  private List<Service> services;

  public Cluster(List<Service> services) {
    this.services = services;
  }

  public void start() throws IOException {
    temp.create();
    if (isUsingExternalCluster()) {
      logger.info("Not starting in-VM cluster: using external cluster");
      return;
    } else {
      logger.info("Starting in-VM cluster");
    }
    for (Service service : services) {
      service.start();
    }
    clusterConfigFile = temp.newFile("kite-env.xml");
    writeConf();
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
    temp.delete();
  }

  public URL getConfigURL() throws MalformedURLException {
    return clusterConfigFile.toURI().toURL();
  }

  public Configuration getConf() {
    // get a new, empty configuration
    Configuration conf = new Configuration(false);
    for (Service service : services) {
      service.configure(conf);
    }
    return conf;
  }

  private void writeConf() throws IOException {
    Configuration clusterConf = getConf();
    OutputStream out = new FileOutputStream(clusterConfigFile);
    clusterConf.writeXml(out);
    out.close();
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
    void configure(Configuration conf);
  }

}
