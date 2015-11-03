package com.liveramp.daemon_lib.demo_daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.builders.ForkingDaemonBuilder;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.DaemonRunner;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.logging.LoggingHelper;

public class DemoDaemon {
  private static final Logger LOG = LoggerFactory.getLogger(DemoDaemon.class);

  public static class DemoJoblet implements Joblet {
    private static final Logger LOG = LoggerFactory.getLogger(DemoJoblet.class);

    private final int id;

    public DemoJoblet(int id) {
      this.id = id;
    }

    @Override
    public void run() throws DaemonException {
      try {
        LOG.info("Running " + id);
        Thread.sleep(100 * 1000);
        LOG.info("Done");
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

  private static class Callbacks implements JobletCallbacks<Config> {
    @Override
    public void before(Config config) throws DaemonException {
      System.out.println("Called before callback for config: " + config.id);
    }

    @Override
    public void after(Config config) throws DaemonException {
      System.out.println("Called after callback for config: " + config.id);
    }
  }

  public static void main(String[] args) throws Exception {
    LoggingHelper.setLoggingProperties("demo-daemon");

    Daemon daemon = new ForkingDaemonBuilder<>(
        "/tmp/daemons",
        "demo",
        Factory.class,
        new Producer(),
        new Callbacks(),
        AlertsHandlers.distribution(DemoDaemon.class)
    ).setMaxProcesses(4).build();

    DaemonRunner.run(daemon);
  }
}
