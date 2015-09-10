package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

@Deprecated
public class DaemonBuilder<T extends JobletConfig> {
  private final String identifier;
  private final JobletExecutor<T> executor;
  private final JobletConfigProducer<T> configProducer;
  private final AlertsHandler alertsHandler;
  private final Daemon.Options options;

  private static final int DEFAULT_SLEEPING_SECONDS = 10;

  public DaemonBuilder(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.executor = executor;
    this.configProducer = configProducer;
    this.alertsHandler = alertsHandler;

    this.options = new Daemon.Options();

    // backward compatibility will remove when I replace usages of setSleepingSeconds
    options.setNextConfigWaitSeconds(DEFAULT_SLEEPING_SECONDS);
  }

  public DaemonBuilder<T> setSleepingSeconds(int sleepingSeconds) {
    options.setNextConfigWaitSeconds(sleepingSeconds);

    return this;
  }

  public Daemon<T> build() {
    return new Daemon<T>(identifier, executor, configProducer, alertsHandler, options);
  }
}
