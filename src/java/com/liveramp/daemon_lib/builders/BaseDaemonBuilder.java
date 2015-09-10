package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public abstract class BaseDaemonBuilder<T extends JobletConfig, K extends BaseDaemonBuilder<T, K>> {
  protected final String identifier;
  private final JobletConfigProducer<T> configProducer;
  private final JobletCallbacks<T> jobletCallbacks;
  private final AlertsHandler alertsHandler;
  private final Daemon.Options options;

  public BaseDaemonBuilder(String identifier, JobletConfigProducer<T> configProducer, JobletCallbacks<T> jobletCallbacks, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.configProducer = configProducer;
    this.jobletCallbacks = jobletCallbacks;
    this.alertsHandler = alertsHandler;

    this.options = new Daemon.Options();

    // backward compatibility will remove when I replace usages of setSleepingSeconds
    this.options.setNextConfigWaitSeconds(10);
  }

  @Deprecated
  @SuppressWarnings("unchecked")
  public K setSleepingSeconds(int sleepingSeconds) {
    options.setNextConfigWaitSeconds(sleepingSeconds);

    return (K)this;
  }

  @SuppressWarnings("unchecked")
  public K setConfigSleepingSeconds(int sleepingSeconds) {
    options.setConfigWaitSeconds(sleepingSeconds);
    return (K)this;
  }

  @SuppressWarnings("unchecked")
  public K setExecutionSlotSleepingSeconds(int sleepingSeconds) {
    options.setExecutionSlotWaitSeconds(sleepingSeconds);
    return (K)this;
  }

  @SuppressWarnings("unchecked")
  public K setMainLoopSleepingSeconds(int sleepingSeconds) {
    options.setNextConfigWaitSeconds(sleepingSeconds);
    return (K)this;
  }

  @NotNull
  protected abstract JobletExecutor<T> getExecutor(JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException;

  public Daemon<T> build() throws IllegalAccessException, IOException, InstantiationException {
    return new Daemon<>(identifier, getExecutor(jobletCallbacks), configProducer, alertsHandler, options);
  }
}
