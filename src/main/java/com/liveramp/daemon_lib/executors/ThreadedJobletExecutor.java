package com.liveramp.daemon_lib.executors;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.config.ExecutorConfig;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.DefaultThreadedExecutionCondition;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionCondition;
import com.liveramp.daemon_lib.utils.DaemonException;

public class ThreadedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadedJobletExecutor.class);

  private final ThreadPoolExecutor threadPool;
  private final JobletFactory<T> jobletFactory;
  private final JobletCallback<T> successCallback;
  private final JobletCallback<T> failureCallback;
  private final Supplier<Config> executorConfigSupplier;

  public ThreadedJobletExecutor(ThreadPoolExecutor threadPool, JobletFactory<T> jobletFactory, JobletCallback<T> successCallback, JobletCallback<T> failureCallback, Supplier<Config> executorConfigSupplier) {
    this.threadPool = threadPool;
    this.jobletFactory = jobletFactory;
    this.successCallback = successCallback;
    this.failureCallback = failureCallback;
    this.executorConfigSupplier = executorConfigSupplier;
  }

  @Override
  public void execute(final T config) throws DaemonException {
    threadPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          Joblet joblet = jobletFactory.create(config);
          joblet.run();
          successCallback.callback(config);
        } catch (Exception e) {
          LOG.error("Failed to call for config " + config, e);
          failureCallback.callback(config);
        }
        return null;
      }
    });
  }

  @Override
  public ExecutionCondition getDefaultExecutionCondition() {
    return new DefaultThreadedExecutionCondition(threadPool, executorConfigSupplier.get());
  }

  @Override
  public void shutdown() {
    threadPool.shutdown();
  }

  public static class Config implements ExecutorConfig {
    public final int numJoblets;

    public Config(int numJoblets) {
      this.numJoblets = numJoblets;
    }
  }
}
