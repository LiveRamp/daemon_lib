package com.liveramp.daemon_lib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.utils.DaemonException;

public class Daemon<T extends JobletConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(Daemon.class);

  private final JobletExecutor<T> executor;
  private final String identifier;
  private final JobletConfigProducer<T> configProducer;
  private boolean running;

  public Daemon(String identifier, JobletExecutor executor, JobletConfigProducer configProducer) {
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
    LOG.info("Attempting to process next joblet");

    if (executor.canExecuteAnother()) {
      T jobletConfig = configProducer.getNextConfig();

      if (jobletConfig != null) {
        LOG.info("Found joblet config: " + jobletConfig);
        executor.execute(jobletConfig);
        return true;
      }
    }

    LOG.info("Too many running joblets");
    return false;
  }

  public final void stop() {
    running = false;
  }

  private void doSleep() {
    try {
      Thread.sleep((long)10000);
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }
}
