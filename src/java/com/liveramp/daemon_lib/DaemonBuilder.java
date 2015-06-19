package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class DaemonBuilder<T extends JobletConfig> {
  private final String identifier;
  private final JobletExecutor<T> executor;
  private final JobletConfigProducer<T> configProducer;
  private final AlertsHandler alertsHandler;
  private int sleepingSeconds;

  private static final int DEFAULT_SLEEPING_SECONDS = 10;

  public DaemonBuilder(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.executor = executor;
    this.configProducer = configProducer;
    this.alertsHandler = alertsHandler;

    this.sleepingSeconds = DEFAULT_SLEEPING_SECONDS;
  }

  public DaemonBuilder<T> setSleepingSeconds(int sleepingSeconds) {
    this.sleepingSeconds = sleepingSeconds;

    return this;
  }

  public Daemon<T> build() {
    return new Daemon<T>(identifier, executor, configProducer, alertsHandler, sleepingSeconds);
  }
}
