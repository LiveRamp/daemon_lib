package com.liveramp.daemon_lib;

import java.util.List;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class DaemonBuilder<T extends JobletConfig> {
  private final String identifier;
  private final JobletExecutor<T> executor;
  private final JobletConfigProducer<T> configProducer;
  private DaemonLock lock;
  private final AlertsHandler alertsHandler;
  private int sleepingSeconds;
  private final List<JobletCallback<T>> preExecutionCallbacks;

  private static final int DEFAULT_SLEEPING_SECONDS = 10;

  DaemonBuilder(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, List<JobletCallback<T>> preExecutionCallbacks, DaemonLock lock, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.executor = executor;
    this.configProducer = configProducer;
    this.lock = lock;
    this.alertsHandler = alertsHandler;

    this.sleepingSeconds = DEFAULT_SLEEPING_SECONDS;
    this.preExecutionCallbacks = preExecutionCallbacks;
  }

  public DaemonBuilder<T> setSleepingSeconds(int sleepingSeconds) {
    this.sleepingSeconds = sleepingSeconds;

    return this;
  }

  public Daemon<T> build() {
    return new Daemon<T>(identifier, executor, configProducer, preExecutionCallbacks, alertsHandler, sleepingSeconds, lock);
  }
}
