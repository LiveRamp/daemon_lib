package com.liveramp.daemon_lib.demo_daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.DaemonBuilders;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class DemoDaemon {

  public static class DemoJoblet implements Joblet {
    private final int id;

    public DemoJoblet(int id) {
      this.id = id;
    }

    @Override
    public void run() throws DaemonException {
      try {
        Thread.sleep(100 * 1000);
      } catch (InterruptedException e) {
        throw new DaemonException(e);
      }
    }
  }

  public static class Config implements JobletConfig {
    private static final long serialVersionUID = 1;
    private final int id;

    public Config(int id) {
      this.id = id;
    }
  }

  public static class Factory implements JobletFactory<Config> {
    @Override
    public Joblet create(Config config) {
      return new DemoJoblet(config.id);
    }
  }

  public static class Producer implements JobletConfigProducer<Config> {
    private int i = 0;

    @Override
    public Config getNextConfig() {
      System.out.println(i);
      return new Config(++i);
    }
  }

  public static void main(String[] args) throws Exception {
    Daemon daemon = DaemonBuilders.forked(
        "/tmp/daemons",
        "demo",
        Factory.class,
        new Producer()
    )
        .setMaxProcesses(4)
        .setConfigWaitSeconds(1)
        .setNextConfigWaitSeconds(1)
        .build();

    daemon.start();
  }
}
