package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class BlockingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, BlockingDaemonBuilder<T>> {

  private final JobletFactory<T> jobletFactory;


  private JobletCallback<T> successCallback;
  private JobletCallback<T> failureCallback;

  public BlockingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer, JobletCallbacks<T> jobletCallbacks, AlertsHandler alertsHandler) {
    super(identifier, configProducer, jobletCallbacks, alertsHandler);
    this.jobletFactory = jobletFactory;

    this.successCallback = new JobletCallback.None<>();
    this.failureCallback = new JobletCallback.None<>();
  }

  public BlockingDaemonBuilder<T> setSuccessCallback(JobletCallback<T> callback) {
    this.successCallback = callback;
    return this;
  }

  public BlockingDaemonBuilder<T> setFailureCallback(JobletCallback<T> callback) {
    this.failureCallback = callback;
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor(JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    return JobletExecutors.Blocking.get(jobletFactory, jobletCallbacks, successCallback, failureCallback);
  }
}
