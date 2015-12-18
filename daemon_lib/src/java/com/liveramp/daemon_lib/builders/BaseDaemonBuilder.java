package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.built_in.NoOpDaemonLock;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public abstract class BaseDaemonBuilder<T extends JobletConfig, K extends BaseDaemonBuilder<T, K>> {
  protected final String identifier;
  private final JobletConfigProducer<T> configProducer;
  protected final AlertsHandler alertsHandler;
  private final Daemon.Options options;
  private JobletCallback<T> onNewConfigCallback;

  public BaseDaemonBuilder(String identifier, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler) {
    this.identifier = identifier;
    this.configProducer = configProducer;
    this.alertsHandler = alertsHandler;
    this.onNewConfigCallback = new JobletCallback.None<>();

    this.options = new Daemon.Options();
  }

  /**
   * See {@link com.liveramp.daemon_lib.Daemon.Options#setConfigWaitSeconds(int)}
   */
  public K setConfigWaitSeconds(int sleepingSeconds) {
    options.setConfigWaitSeconds(sleepingSeconds);
    return self();
  }

  /**
   * See {@link com.liveramp.daemon_lib.Daemon.Options#setExecutionSlotWaitSeconds(int)}
   */
  public K setExecutionSlotWaitSeconds(int sleepingSeconds) {
    options.setExecutionSlotWaitSeconds(sleepingSeconds);
    return self();
  }

  /**
   * See {@link com.liveramp.daemon_lib.Daemon.Options#setNextConfigWaitSeconds(int)}
   */
  public K setNextConfigWaitSeconds(int sleepingSeconds) {
    options.setNextConfigWaitSeconds(sleepingSeconds);
    return self();
  }

  /**
   * See {@link com.liveramp.daemon_lib.Daemon.Options#setFailureWaitSeconds(int)}
   */
  public K setFailureWaitSeconds(int sleepingSeconds) {
    options.setFailureWaitSeconds(sleepingSeconds);
    return self();
  }

  public K setOnNewConfigCallback(JobletCallback<T> callback) {
    this.onNewConfigCallback = callback;
    return self();
  }

  @SuppressWarnings("unchecked")
  private K self() {
    return (K)this;
  }

  @NotNull
  protected abstract JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException;

  public Daemon<T> build() throws IllegalAccessException, IOException, InstantiationException {
    return new Daemon<>(identifier, getExecutor(), configProducer, onNewConfigCallback, new NoOpDaemonLock(), alertsHandler, options);
  }
}
