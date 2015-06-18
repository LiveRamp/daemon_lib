package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class DaemonBuilder<T extends JobletConfig> {
  private final String identifier;
  private final JobletExecutor<T> executor;
  private final JobletConfigProducer<T> configProducer;
  private final AlertsHandler alertsHandler;
  private Integer sleepingSeconds;

  public DaemonBuilder(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.executor = executor;
    this.configProducer = configProducer;
    this.alertsHandler = alertsHandler;

    this.sleepingSeconds = null;
  }

  public DaemonBuilder<T> setSleepingSeconds(Integer sleepingSeconds) {
    this.sleepingSeconds = sleepingSeconds;

    return this;
  }

  public Daemon<T> build() {
    return new Daemon<T>(identifier, executor, configProducer, alertsHandler, sleepingSeconds);
  }
}
