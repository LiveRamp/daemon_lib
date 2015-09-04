package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public abstract class BaseDaemonBuilder<T extends JobletConfig, K extends BaseDaemonBuilder<T, K>> {
  protected final String identifier;
  private final Class<? extends JobletFactory<T>> jobletFactoryClass;
  private final JobletConfigProducer<T> configProducer;
  private final JobletCallbacks<T> jobletCallbacks;
  private final AlertsHandler alertsHandler;
  private int sleepingSeconds;

  private static final int DEFAULT_SLEEPING_SECONDS = 10;

  public BaseDaemonBuilder(String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, JobletCallbacks<T> jobletCallbacks, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.jobletFactoryClass = jobletFactoryClass;
    this.configProducer = configProducer;
    this.jobletCallbacks = jobletCallbacks;
    this.alertsHandler = alertsHandler;

    this.sleepingSeconds = DEFAULT_SLEEPING_SECONDS;
  }

  @SuppressWarnings("unchecked")
  public K setSleepingSeconds(int sleepingSeconds) {
    this.sleepingSeconds = sleepingSeconds;

    return (K)this;
  }

  @NotNull protected abstract JobletExecutor<T> getExecutor(Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException;

  public Daemon<T> build() throws IllegalAccessException, IOException, InstantiationException {
    return new Daemon<T>(identifier, getExecutor(jobletFactoryClass, jobletCallbacks), configProducer, alertsHandler, sleepingSeconds);
  }
}
