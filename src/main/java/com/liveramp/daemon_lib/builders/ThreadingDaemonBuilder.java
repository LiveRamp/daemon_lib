package com.liveramp.daemon_lib.builders;

import java.io.IOException;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.daemon_lib.executors.ThreadedJobletExecutor;
import com.liveramp.daemon_lib.executors.config.ExecutorConfigSuppliers;

public class ThreadingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, ThreadingDaemonBuilder<T>> {

  private final JobletFactory<T> jobletFactory;
  private JobletCallback<T> successCallback;
  private JobletCallback<T> failureCallback;

  private static final int DEFAULT_MAX_THREADS = 1;
  private Supplier<ThreadedJobletExecutor.Config> executorConfigSupplier;
  private int maxThreads;

  public ThreadingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer) {
    super(identifier, configProducer);
    this.jobletFactory = jobletFactory;

    this.maxThreads = DEFAULT_MAX_THREADS;
    this.executorConfigSupplier = () -> new ThreadedJobletExecutor.Config(DEFAULT_MAX_THREADS);

    this.successCallback = new JobletCallback.None<>();
    this.failureCallback = new JobletCallback.None<>();
  }

  public ThreadingDaemonBuilder<T> setMaxThreads(int maxThreads) {
    Preconditions.checkArgument(maxThreads > 0);
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

  public ThreadingDaemonBuilder<T> setExecutorConfigSupplier(Supplier<ThreadedJobletExecutor.Config> executorConfigSupplier) {
    this.executorConfigSupplier = executorConfigSupplier;
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    final Supplier<ThreadedJobletExecutor.Config> configSupplier = ExecutorConfigSuppliers.fallingBack(executorConfigSupplier, () -> new ThreadedJobletExecutor.Config(maxThreads));
    return JobletExecutors.Threaded.get(jobletFactory, successCallback, failureCallback, configSupplier);
  }
}
