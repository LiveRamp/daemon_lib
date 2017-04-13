package com.liveramp.daemon_lib.builders;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.ForkedJobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.daemon_lib.executors.config.ExecutorConfigSuppliers;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;

public class ForkingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, ForkingDaemonBuilder<T>> {

  private final String workingDir;
  private final Class<? extends JobletFactory<T>> jobletFactoryClass;
  private Map<String, String> envVariables;
  private JobletCallback<? super T> successCallback;
  private JobletCallback<? super T> failureCallback;
  private ProcessJobletRunner jobletRunner;

  private static final int DEFAULT_MAX_PROCESSES = 1;
  private static final Map<String, String> DEFAULT_ENV_VARS = Maps.newHashMap();
  private Supplier<ForkedJobletExecutor.Config> executorConfigSupplier;
  private int maxProcesses;

  public ForkingDaemonBuilder(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, ProcessJobletRunner jobletRunner) {
    super(identifier, configProducer);
    this.workingDir = workingDir;
    this.jobletFactoryClass = jobletFactoryClass;
    this.jobletRunner = jobletRunner;

    maxProcesses = DEFAULT_MAX_PROCESSES;
    envVariables = DEFAULT_ENV_VARS;
    successCallback = new JobletCallback.None<>();
    failureCallback = new JobletCallback.None<>();

    executorConfigSupplier = null;
  }

  public ForkingDaemonBuilder<T> setMaxProcesses(int maxProcesses) {
    this.maxProcesses = maxProcesses;
    return this;
  }

  public ForkingDaemonBuilder<T> addToEnvironmentVariables(String envVar, String value) {
    this.envVariables.put(envVar, value);
    return this;
  }

  public ForkingDaemonBuilder<T> setSuccessCallback(JobletCallback<? super T> callback) {
    this.successCallback = callback;
    return this;
  }

  public ForkingDaemonBuilder<T> setFailureCallback(JobletCallback<? super T> callback) {
    this.failureCallback = callback;
    return this;
  }

  public ForkingDaemonBuilder<T> setExecutorConfigSupplier(Supplier<ForkedJobletExecutor.Config> executorConfigSupplier) {
    this.executorConfigSupplier = executorConfigSupplier;
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    final Supplier<ForkedJobletExecutor.Config> defaultConfigSupplier = () -> new ForkedJobletExecutor.Config(maxProcesses);
    final Supplier<ForkedJobletExecutor.Config> compositeConfigSupplier = executorConfigSupplier == null ? defaultConfigSupplier : ExecutorConfigSuppliers.fallingBack(executorConfigSupplier, defaultConfigSupplier);
    return JobletExecutors.Forked.get(notifier, tmpPath, jobletFactoryClass, envVariables, successCallback, failureCallback, jobletRunner, compositeConfigSupplier);
  }
}
