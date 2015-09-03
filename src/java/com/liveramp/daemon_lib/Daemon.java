package com.liveramp.daemon_lib;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.alerts_handler.recipients.AlertSeverity;

public class Daemon<T extends JobletConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(Daemon.class);

  private final String identifier;
  private final JobletExecutor<T> executor;
  private final AlertsHandler alertsHandler;
  private final JobletConfigProducer<T> configProducer;
  private final int sleepingSeconds;

  private boolean running;
  private final JobletCallback<T> preExecutionCallback;
  private DaemonLock lock;

  public Daemon(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, JobletCallback<T> preExecutionCallback, AlertsHandler alertsHandler, int sleepingSeconds, DaemonLock lock) {
    this.preExecutionCallback = preExecutionCallback;
    this.lock = lock;
    this.identifier = clean(identifier);
    this.configProducer = configProducer;
    this.executor = executor;
    this.alertsHandler = alertsHandler;
    this.sleepingSeconds = sleepingSeconds;
    this.running = false;
  }

  private static String clean(String identifier) {
    return identifier.replaceAll("\\s", "-");
  }

  public final void start() {
    LOG.info("Starting daemon ({})", identifier);
    running = true;

    try {
      while (running) {
        processNext();
        doSleep();
      }
    } catch (Exception e) {
      alertsHandler.sendAlert("Fatal error occurred in daemon (" + identifier + "). Shutting down.", e, AlertRecipients.engineering(AlertSeverity.ERROR));
      throw e;
    }
    LOG.info("Exiting daemon ({})", identifier);
  }

  protected boolean processNext() {
    if (executor.canExecuteAnother()) {
      T jobletConfig;
      try {
        lock.lock();
        jobletConfig = configProducer.getNextConfig();
      } catch (DaemonException e) {
        alertsHandler.sendAlert("Error getting next config for daemon (" + identifier + ")", e, AlertRecipients.engineering(AlertSeverity.ERROR));
        return false;
      }

      if (jobletConfig != null) {
        LOG.info("Found joblet config: " + jobletConfig);
        try {
          preExecutionCallback.callback(jobletConfig);
          executor.execute(jobletConfig);
        } catch (Exception e) {
          alertsHandler.sendAlert("Error executing joblet config for daemon (" + identifier + ")", jobletConfig.toString(), e, AlertRecipients.engineering(AlertSeverity.ERROR));
        }
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
      if (sleepingSeconds > 0) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(sleepingSeconds));
      }
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }
}
