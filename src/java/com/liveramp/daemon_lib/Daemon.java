package com.liveramp.daemon_lib;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.utils.DaemonException;

public class Daemon<T extends JobletConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(Daemon.class);

  private final String identifier;
  private final JobletExecutor<T> executor;
  private final JobletConfigProducer<T> configProducer;

  private boolean running;

  public Daemon(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer) {
    this.identifier = identifier;
    this.configProducer = configProducer;
    this.executor = executor;
    this.running = false;
  }

  public final void start() throws DaemonException {
    LOG.info("Starting daemon ({})", identifier);
    running = true;

    while (running) {
      processNext();
      doSleep();
    }

    LOG.info("Exiting daemon ({})", identifier);
  }

  protected boolean processNext() throws DaemonException {
    if (executor.canExecuteAnother()) {
      T jobletConfig = configProducer.getNextConfig();

      if (jobletConfig != null) {
        LOG.info("Found joblet config: " + jobletConfig);
        executor.execute(jobletConfig);
        return true;
      }
    }

    return false;
  }

  public final void stop() {
    running = false;
  }

  public String getIdentifier() {
    return identifier;
  }

  private void doSleep() {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }
}
