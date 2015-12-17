package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class ThreadingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, ThreadingDaemonBuilder<T>> {

  private final JobletFactory<T> jobletFactory;
  private JobletCallback<T> successCallback;
  private JobletCallback<T> failureCallback;
  private int maxThreads;

  private static final int DEFAULT_MAX_THREADS = 1;


  public ThreadingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer, AlertsHandler alertsHandler) {
    super(identifier, configProducer, alertsHandler);
    this.jobletFactory = jobletFactory;

    this.maxThreads = DEFAULT_MAX_THREADS;

    this.successCallback = new JobletCallback.None<>();
    this.failureCallback = new JobletCallback.None<>();
  }

  public ThreadingDaemonBuilder<T> setMaxThreads(int maxThreads) {
    this.maxThreads = maxThreads;
    return this;
  }

  public ThreadingDaemonBuilder<T> setSuccessCallback(JobletCallback<T> successCallback) {
    this.successCallback = successCallback;
    return this;
  }

  public ThreadingDaemonBuilder<T> setFailureCallback(JobletCallback<T> failureCallback) {
    this.failureCallback = failureCallback;
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    return JobletExecutors.Threaded.get(maxThreads, jobletFactory, successCallback, failureCallback);
  }
}
