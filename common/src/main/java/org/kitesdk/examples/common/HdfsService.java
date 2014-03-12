package org.kitesdk.examples.common;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

class HdfsService implements Cluster.Service {

  private MiniDFSCluster hdfsCluster;

  @Override
  public void start() throws IOException {
    new MiniDFSCluster.Builder(new Configuration()).nameNodePort(8020).build();
  }

  @Override
  public void stop() throws IOException {
    if (hdfsCluster != null) {
      hdfsCluster.shutdown();
      hdfsCluster = null;
    }
  }
}
