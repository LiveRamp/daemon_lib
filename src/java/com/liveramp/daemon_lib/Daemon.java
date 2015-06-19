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

  public Daemon(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler, int sleepingSeconds) {
    this.identifier = identifier;
    this.configProducer = configProducer;
    this.executor = executor;
    this.alertsHandler = alertsHandler;
    this.sleepingSeconds = sleepingSeconds;
    this.running = false;
  }

  public final void start() {
    LOG.info("Starting daemon ({})", identifier);
    running = true;

    while (running) {
      processNext();
      doSleep();
    }

    LOG.info("Exiting daemon ({})", identifier);
  }

  protected boolean processNext() {
    if (executor.canExecuteAnother()) {
      T jobletConfig;
      try {
        jobletConfig = configProducer.getNextConfig();
      } catch (DaemonException e) {
        alertsHandler.sendAlert("Error getting next config for daemon (" + identifier + ")", e, AlertRecipients.engineering(AlertSeverity.ERROR));
        return false;
      }

      if (jobletConfig != null) {
        LOG.info("Found joblet config: " + jobletConfig);
        try {
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
      Thread.sleep(TimeUnit.SECONDS.toMillis(sleepingSeconds));
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }
}
