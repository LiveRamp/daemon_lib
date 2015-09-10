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

  private static final int DEFAULT_CONFIG_WAIT_SECONDS = 1;
  private static final int DEFAULT_EXECUTION_SLOT_WAIT_SECONDS = 1;
  private static final int DEFAULT_NEXT_CONFIG_WAIT_SECONDS = 0;

  public static class Options {
    private int configWaitSeconds = DEFAULT_CONFIG_WAIT_SECONDS;
    private int executionSlotWaitSeconds = DEFAULT_EXECUTION_SLOT_WAIT_SECONDS;
    private int nextConfigWaitSeconds = DEFAULT_NEXT_CONFIG_WAIT_SECONDS;

    // How long the daemon should wait before retrying when there is not config available.
    public Options setConfigWaitSeconds(int sleepingSeconds) {
      this.configWaitSeconds = sleepingSeconds;
      return this;
    }

    // How long the daemon should wait before retrying when the max number of running joblets is reached.
    public Options setExecutionSlotWaitSeconds(int sleepingSeconds) {
      this.executionSlotWaitSeconds = sleepingSeconds;
      return this;
    }

    // How long the daemon should wait before fetching the next config.
    public Options setNextConfigWaitSeconds(int sleepingSeconds) {
      this.nextConfigWaitSeconds = sleepingSeconds;
      return this;
    }
  }

  private final String identifier;
  private final JobletExecutor<T> executor;
  private final AlertsHandler alertsHandler;
  private final JobletConfigProducer<T> configProducer;

  private final Options options;

  private boolean running;

  public Daemon(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler, Options options) {
    this.identifier = clean(identifier);
    this.configProducer = configProducer;
    this.executor = executor;
    this.alertsHandler = alertsHandler;
    this.options = options;

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
        silentSleep(options.nextConfigWaitSeconds);
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
      } else {
        silentSleep(options.configWaitSeconds);
      }
    } else {
      silentSleep(options.executionSlotWaitSeconds);
    }

    return false;
  }

  public final void stop() {
    running = false;
  }

  public String getIdentifier() {
    return identifier;
  }

  private void silentSleep(int seconds) {
    try {
      if (seconds > 0) {
        TimeUnit.SECONDS.sleep(seconds);
      }
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }
}
