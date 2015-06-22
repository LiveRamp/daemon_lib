package com.liveramp.daemon_lib;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.Maps;

import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class Daemons {
  public static <T extends JobletConfig> Daemon<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IOException, InstantiationException, IllegalAccessException {
    return getBuilder(workingDir, identifier, maxProcess, jobletFactoryClass, jobletConfigProducer, alertsHandler, jobletCallbacks)
        .build();
  }

  public static <T extends JobletConfig> Daemon<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks, int sleepingSeconds) throws IOException, InstantiationException, IllegalAccessException {
    return getBuilder(workingDir, identifier, maxProcess, jobletFactoryClass, jobletConfigProducer, alertsHandler, jobletCallbacks)
        .setSleepingSeconds(sleepingSeconds)
        .build();
  }

  public static <T extends JobletConfig> Daemon<T> blocking(String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws InstantiationException, IllegalAccessException {
    return new DaemonBuilder<>(
        identifier,
        JobletExecutors.Blocking.get(jobletFactoryClass, jobletCallbacks),
        jobletConfigProducer,
        alertsHandler)
        .build();
  }

  private static <T extends JobletConfig> DaemonBuilder<T> getBuilder(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return new DaemonBuilder<>(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactoryClass, jobletCallbacks, Maps.<String, String>newHashMap()),
        jobletConfigProducer,
        alertsHandler);
  }
}
