package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;

public abstract class BaseThreadingDaemonBuilder<T extends JobletConfig, E extends BaseThreadingDaemonBuilder<T, E>> extends BaseDaemonBuilder<T, E> {

  private final JobletFactory<T> jobletFactory;
  private JobletCallback<T> successCallback;
  private JobletCallback<T> failureCallback;
  private int maxThreads;

  private static final int DEFAULT_MAX_THREADS = 1;

  public BaseThreadingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer) {
    super(identifier, configProducer);
    this.jobletFactory = jobletFactory;

    this.maxThreads = DEFAULT_MAX_THREADS;

    this.successCallback = new JobletCallback.None<>();
    this.failureCallback = new JobletCallback.None<>();
  }

  public BaseThreadingDaemonBuilder<T, E> setMaxThreads(int maxThreads) {
    this.maxThreads = maxThreads;
    return self();
  }

  public BaseThreadingDaemonBuilder<T, E> setSuccessCallback(JobletCallback<T> successCallback) {
    this.successCallback = successCallback;
    return self();
  }

  public BaseThreadingDaemonBuilder<T, E> setFailureCallback(JobletCallback<T> failureCallback) {
    this.failureCallback = failureCallback;
    return self();
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    return JobletExecutors.Threaded.get(maxThreads, jobletFactory, successCallback, failureCallback);
  }

}
