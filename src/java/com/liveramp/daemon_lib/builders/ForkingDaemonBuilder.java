package com.liveramp.daemon_lib.builders;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class ForkingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, ForkingDaemonBuilder<T>> {

  private final String workingDir;
  private int maxProcesses;
  private Map<String,String> envVariables;

  private static final int DEFAULT_MAX_PROCESSES = 1;
  private static final Map<String, String> DEFAULT_ENV_VARS = Maps.newHashMap();


  public ForkingDaemonBuilder(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, JobletCallbacks<T> jobletCallbacks, AlertsHandler alertsHandler) {
    super(identifier, jobletFactoryClass, configProducer, jobletCallbacks, alertsHandler);
    this.workingDir = workingDir;

    maxProcesses = DEFAULT_MAX_PROCESSES;
    envVariables = DEFAULT_ENV_VARS;
  }

  public ForkingDaemonBuilder<T> setMaxProcesses(int maxProcesses) {
    this.maxProcesses = maxProcesses;
    return this;
  }

  public ForkingDaemonBuilder<T> addToEnvironmentVariables(String envVar, String value) {
    this.envVariables.put(envVar, value);
    return this;
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor(Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return JobletExecutors.Forked.get(tmpPath, maxProcesses, jobletFactoryClass, jobletCallbacks, envVariables);
  }

}
