package com.liveramp.daemon_lib.demo_daemon;

import java.io.IOException;
import java.nio.file.Paths;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.DaemonBuilders;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.ThreadedJobletExecutor;
import com.liveramp.daemon_lib.executors.config.ExecutorConfigSuppliers;
import com.liveramp.daemon_lib.executors.forking.JarBasedProcessJobletRunner;
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
        final String startmsg = "Started: " + id;
        final String endMsg = "Ending: " + id;
        System.out.println(startmsg);
        Thread.sleep(3 * 1000);
        System.out.println(endMsg);
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

    @Override
    public String toString() {
      return String.format("{ \"className\": \"%s\", \"id\": %d }", this.getClass().getCanonicalName(), id);
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

  public static class SuccessCallback<T extends JobletConfig> implements JobletCallback<T> {
    @Override
    public void callback(T config) throws DaemonException {
      System.out.println("Success " + config);
    }
  }

  public static class FailureCallback<T extends JobletConfig> implements JobletCallback<T> {
    @Override
    public void callback(T config) throws DaemonException {
      System.out.println("Failure " + config);
    }
  }

  public static Daemon getFileBasedConfigDaemon() throws IllegalAccessException, IOException, InstantiationException {
    Daemon daemon = DaemonBuilders.threaded(
        "threaded",
        new Factory(),
        new Producer()
    )
        .setExecutorConfigSupplier(ExecutorConfigSuppliers.standard(Paths.get("/tmp/daemon-info"), ThreadedJobletExecutor.Config.class))
        .setSuccessCallback(new SuccessCallback<>())
        .setFailureCallback(new FailureCallback<>())
        .setConfigWaitSeconds(1)
        .setNextConfigWaitSeconds(1)
        .build();

    return daemon;
  }

  public static Daemon getFixedConfigDaemon() throws IllegalAccessException, IOException, InstantiationException {
    Daemon daemon = DaemonBuilders.forked(
        "/tmp/daemons",
        "demo",
        Factory.class,
        new Producer(),
        JarBasedProcessJobletRunner.builder(".").build()
    )
        .setSuccessCallback(new SuccessCallback<>())
        .setFailureCallback(new FailureCallback<>())
        .setConfigWaitSeconds(1)
        .setNextConfigWaitSeconds(1)
        .build();

    return daemon;
  }

  public static void main(String[] args) throws Exception {
    Daemon daemon = getFileBasedConfigDaemon();
    daemon.start();
  }
}
