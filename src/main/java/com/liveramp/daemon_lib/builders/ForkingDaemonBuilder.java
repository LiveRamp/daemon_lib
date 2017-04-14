package com.liveramp.daemon_lib.builders;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.config_serialization.JavaJobletConfigSerializer;
import com.liveramp.daemon_lib.config_serialization.JobletConfigSerializer;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;

public class ForkingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, ForkingDaemonBuilder<T>> {

  private final String workingDir;
  private final Class<? extends JobletFactory<T>> jobletFactoryClass;
  private int maxProcesses;
  private Map<String, String> envVariables;
  private JobletCallback<? super T> successCallback;
  private JobletCallback<? super T> failureCallback;
  private ProcessJobletRunner jobletRunner;
  private JobletConfigSerializer<T> jobletConfigSerializer;

  private static final int DEFAULT_MAX_PROCESSES = 1;
  private static final Map<String, String> DEFAULT_ENV_VARS = Maps.newHashMap();

  public ForkingDaemonBuilder(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, ProcessJobletRunner jobletRunner) {
    super(identifier, configProducer);
    this.workingDir = workingDir;
    this.jobletFactoryClass = jobletFactoryClass;
    this.jobletRunner = jobletRunner;

    maxProcesses = DEFAULT_MAX_PROCESSES;
    envVariables = DEFAULT_ENV_VARS;
    successCallback = new JobletCallback.None<>();
    failureCallback = new JobletCallback.None<>();
    jobletConfigSerializer = new JavaJobletConfigSerializer<>();
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

  public ForkingDaemonBuilder<T> setJobletConfigSerializer(JobletConfigSerializer<T> jobletConfigSerializer) {
    this.jobletConfigSerializer = jobletConfigSerializer;
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return JobletExecutors.Forked.get(notifier, tmpPath, maxProcesses, jobletFactoryClass, envVariables, successCallback, failureCallback, jobletRunner, jobletConfigSerializer);
  }
}
