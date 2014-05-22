package org.kitesdk.examples.common;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;

class HiveMetastoreService implements Cluster.Service {

  private Map<String, String> sysProps = Maps.newHashMap();
  private Thread serverThread;

  @Override
  public void start() throws IOException {
    final HiveConf serverConf = new HiveConf(new Configuration(), this.getClass());
    serverConf.set("hive.metastore.local", "false");
    serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:target/metastore_db;create=true");
    //serverConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname, NotificationListener.class.getName());
    File derbyLogFile = new File("target/derby.log");
    derbyLogFile.createNewFile();
    setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());
    serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(9083, ShimLoader.getHadoopThriftAuthBridge(),
              serverConf);
          //LOG.info("Started metastore server on port " + msPort);
        }
        catch (Throwable e) {
          //LOG.error("Metastore Thrift Server threw an exception...", e);
        }
      }
    });
    serverThread.setDaemon(true);
    serverThread.start();
    try {
      Thread.sleep(10000L);
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  @Override
  public void stop() throws IOException {
    resetSystemProperties();
    serverThread.stop();
  }

  public void configure(Configuration conf) {
    conf.set("hive.metastore.uris", "thrift://localhost.localdomain:9083");
  }

  private void setSystemProperty(String name, String value) {
    if (!sysProps.containsKey(name)) {
      String currentValue = System.getProperty(name);
      sysProps.put(name, currentValue);
    }
    if (value != null) {
      System.setProperty(name, value);
    }
    else {
      System.getProperties().remove(name);
    }
  }

  private void resetSystemProperties() {
    for (Map.Entry<String, String> entry : sysProps.entrySet()) {
      if (entry.getValue() != null) {
        System.setProperty(entry.getKey(), entry.getValue());
      }
      else {
        System.getProperties().remove(entry.getKey());
      }
    }
    sysProps.clear();
  }
}
