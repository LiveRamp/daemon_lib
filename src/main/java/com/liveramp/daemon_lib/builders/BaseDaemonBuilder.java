package com.liveramp.daemon_lib.builders;

import com.liveramp.daemon_lib.DaemonCallback;
import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.daemon_lib.DaemonLock;
import com.liveramp.daemon_lib.DaemonNotifier;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.built_in.NoOpDaemonLock;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.postconfig.ConfigBasedExecutionCondition;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.postconfig.ConfigBasedExecutionConditions;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionCondition;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionConditions;
import com.liveramp.daemon_lib.utils.LoggingForwardingNotifier;
import com.liveramp.daemon_lib.utils.NoOpDaemonNotifier;

public abstract class BaseDaemonBuilder<T extends JobletConfig, K extends BaseDaemonBuilder<T, K>> {
  protected final String identifier;
  private final JobletConfigProducer<T> configProducer;
  protected DaemonNotifier notifier;
  private final Daemon.Options options;
  private JobletCallback<? super T> onNewConfigCallback;
  protected JobletCallback<? super T> successCallback;
  protected JobletCallback<? super T> failureCallback;
  private DaemonCallback wakeUpCallback;
  private DaemonLock lock;
  protected ExecutionCondition additionalExecutionCondition = ExecutionConditions.alwaysExecute();
  protected ConfigBasedExecutionCondition<T> postConfigExecutionCondition = ConfigBasedExecutionConditions.alwaysExecute();

  public BaseDaemonBuilder(String identifier, JobletConfigProducer<T> configProducer) {
    this.identifier = identifier;
    this.configProducer = configProducer;
    this.onNewConfigCallback = new JobletCallback.None<>();
    this.successCallback = new JobletCallback.None<>();
    this.failureCallback = new JobletCallback.None<>();
    this.wakeUpCallback = new DaemonCallback.None();
    this.lock = new NoOpDaemonLock();

    this.options = new Daemon.Options();
    this.notifier = new NoOpDaemonNotifier();
  }

  public K setNotifier(DaemonNotifier notifier) {
    this.notifier = notifier;
    return self();
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

  /**
   * The callback that should be immediately after a new config is produced.
   */
  public K setOnNewConfigCallback(JobletCallback<? super T> callback) {
    this.onNewConfigCallback = callback;
    return self();
  }

  /**
   * The callback that gets executed each time the daemon wakes up.
   */
  public K setWakeUpCallback(DaemonCallback callback) {
    this.wakeUpCallback = callback;
    return self();
  }

  /**
   * The callback that gets executed when the joblet succeeds.
   */
  public K setSuccessCallback(JobletCallback<? super T> callback) {
    this.successCallback = callback;
    return self();
  }

  /**
   * The callback that gets executed when the joblet fails.
   */
  public K setFailureCallback(JobletCallback<? super T> callback) {
    this.failureCallback = callback;
    return self();
  }

  /**
   * The synchronization mechanism that ensures only one {@link Daemon}
   * instance produces a configuration at a time.
   */
  public K setDaemonConfigProductionLock(DaemonLock lock) {
    this.lock = lock;
    return self();
  }

  public K setAdditionalPreConfigExecutionCondition(ExecutionCondition executionCondition) {
    this.additionalExecutionCondition = executionCondition;
    return self();
  }

  public K setPostConfigExecutionCondition(ConfigBasedExecutionCondition<T> configBasedExecutionCondition) {
    this.postConfigExecutionCondition = configBasedExecutionCondition;
    return self();
  }

  protected abstract K self();

  @NotNull
  protected abstract JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException;

  @NotNull
  public Daemon<T> build() throws IllegalAccessException, IOException, InstantiationException {
    final JobletExecutor<T> executor = getExecutor();
    LoggingForwardingNotifier notifier = new LoggingForwardingNotifier(this.notifier);
    return new Daemon<>(identifier, executor, configProducer, onNewConfigCallback, lock, notifier, options, ExecutionConditions.and(executor.getDefaultExecutionCondition(), additionalExecutionCondition), postConfigExecutionCondition, wakeUpCallback);
  }
}
