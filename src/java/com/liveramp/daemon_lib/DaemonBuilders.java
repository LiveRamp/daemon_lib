package com.liveramp.daemon_lib;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.Maps;

import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class DaemonBuilders {
  public static <T extends JobletConfig> DaemonBuilder<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return new DaemonBuilder<>(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactoryClass, jobletCallbacks, Maps.<String, String>newHashMap()),
        jobletConfigProducer,
        alertsHandler);
  }

  public static <T extends JobletConfig> DaemonBuilder<T> blocking(String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws InstantiationException, IllegalAccessException {
    return new DaemonBuilder<>(
        identifier,
        JobletExecutors.Blocking.get(jobletFactoryClass, jobletCallbacks),
        jobletConfigProducer,
        alertsHandler);
  }
}
