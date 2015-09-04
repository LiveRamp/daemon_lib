package com.liveramp.daemon_lib.builders;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.java_support.alerts_handler.AlertsHandler;

public class BlockingDaemonBuilder<T extends JobletConfig> extends BaseDaemonBuilder<T, BlockingDaemonBuilder<T>> {
  public BlockingDaemonBuilder(String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, JobletCallbacks<T> jobletCallbacks, AlertsHandler alertsHandler) {
    super(identifier, jobletFactoryClass, configProducer, jobletCallbacks, alertsHandler);
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor(Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallbacks<T> jobletCallbacks) throws IllegalAccessException, IOException, InstantiationException {
    return JobletExecutors.Blocking.get(jobletFactoryClass, jobletCallbacks);
  }
}
