package com.liveramp.daemon_lib;

import java.io.File;
import java.io.IOException;

import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class Daemons {
  public static <T extends JobletConfig> Daemon<T> forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IOException, InstantiationException, IllegalAccessException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return new Daemon<T>(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactoryClass, jobletCallbacks),
        jobletConfigProducer,
        alertsHandler
    );
  }
}
