package com.liveramp.daemon_lib.executors;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class ThreadedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadedJobletExecutor.class);

  private final ThreadPoolExecutor threadPool;
  private final JobletFactory<T> jobletFactory;
  private final JobletCallbacks<T> jobletCallbacks;

  public ThreadedJobletExecutor(ThreadPoolExecutor threadPool, JobletFactory<T> jobletFactory, JobletCallbacks<T> jobletCallbacks) {
    this.threadPool = threadPool;
    this.jobletFactory = jobletFactory;
    this.jobletCallbacks = jobletCallbacks;
  }

  @Override
  public void execute(final T config) throws DaemonException {
    jobletCallbacks.before(config);

    threadPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          Joblet joblet = jobletFactory.create(config);
          joblet.run();
        } catch (Exception e) {
          LOG.error("Failed to call for config {}", config, e);
        } finally {
          jobletCallbacks.after(config);
        }
        return null;
      }
    });
  }

  @Override
  public boolean canExecuteAnother() {
    return threadPool.getActiveCount() < threadPool.getMaximumPoolSize();
  }

  @Override
  public void shutdown() {
    threadPool.shutdown();
  }
}
