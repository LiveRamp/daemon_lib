package com.liveramp.daemon_lib.demo_daemon;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.Daemons;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;
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

    @Override
    public void onComplete() {
      System.out.println("Complete");
    }

    public static class Config implements JobletConfig {
      private static final long serialVersionUID = 1;
      private final int id;

      public Config(int id) {
        this.id = id;
      }
    }

    public static class Factory extends JobletFactory<Config> {
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
  }

  public static void main(String[] args) throws DaemonException, IOException {
    LoggingHelper.setLoggingProperties("demo-daemon");
    LoggingHelper.configureConsoleLogging();

    Daemon daemon = Daemons.forked("demo", 4, new DemoJoblet.Factory(), new DemoJoblet.Producer());
    daemon.start();
  }
}
