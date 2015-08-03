package com.liveramp.daemon_lib;

import java.io.IOException;

import com.liveramp.java_support.alerts_handler.AlertsHandler;

@Deprecated
public class Daemons {
  public static <T extends JobletConfig> Daemon<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IOException, InstantiationException, IllegalAccessException {
    return DaemonBuilders.forked(workingDir, identifier, maxProcess, jobletFactoryClass, jobletConfigProducer, alertsHandler, jobletCallbacks)
        .build();
  }

  public static <T extends JobletConfig> Daemon<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks, int sleepingSeconds) throws IOException, InstantiationException, IllegalAccessException {
    return DaemonBuilders.forked(workingDir, identifier, maxProcess, jobletFactoryClass, jobletConfigProducer, alertsHandler, jobletCallbacks)
        .setSleepingSeconds(sleepingSeconds)
        .build();
  }

  public static <T extends JobletConfig> Daemon<T> blocking(String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws InstantiationException, IllegalAccessException {
    return DaemonBuilders.blocking(identifier, jobletFactoryClass, jobletConfigProducer, alertsHandler, jobletCallbacks)
        .build();
  }
}
