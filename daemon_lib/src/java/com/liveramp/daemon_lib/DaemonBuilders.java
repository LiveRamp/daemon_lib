package com.liveramp.daemon_lib;

import java.io.IOException;

import com.liveramp.daemon_lib.builders.BlockingDaemonBuilder;
import com.liveramp.daemon_lib.builders.ForkingDaemonBuilder;
import com.liveramp.daemon_lib.builders.ThreadingDaemonBuilder;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class DaemonBuilders {

  public static <T extends JobletConfig> ForkingDaemonBuilder<T> forked(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler) throws IllegalAccessException, IOException, InstantiationException {
    return new ForkingDaemonBuilder<>(
        workingDir,
        identifier,
        jobletFactoryClass,
        jobletConfigProducer,
        alertsHandler
    );
  }

  public static <T extends JobletConfig> BlockingDaemonBuilder<T> blocking(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler) throws InstantiationException, IllegalAccessException {
    return new BlockingDaemonBuilder<>(
        identifier,
        jobletFactory,
        jobletConfigProducer,
        new JobletCallbacks.None<T>(),
        alertsHandler);
  }

  public static <T extends JobletConfig> ThreadingDaemonBuilder<T> threaded(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    return new ThreadingDaemonBuilder<>(
        identifier,
        jobletFactory,
        jobletConfigProducer,
        jobletCallbacks,
        alertsHandler
    );
  }

  public static <T extends JobletConfig> ThreadingDaemonBuilder<T> threaded(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> jobletConfigProducer, AlertsHandler alertsHandler) throws IllegalAccessException, IOException, InstantiationException {
    return new ThreadingDaemonBuilder<>(
        identifier,
        jobletFactory,
        jobletConfigProducer,
        new JobletCallbacks.None<T>(),
        alertsHandler
    );
  }
}
