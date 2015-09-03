package com.liveramp.daemon_lib.executors;

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
    threadPool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          try {
            jobletCallbacks.before(config);
            Joblet joblet = jobletFactory.create(config);
            joblet.run();
          } finally {
            jobletCallbacks.after(config);
          }
        } catch (DaemonException e) {
          LOG.error("Failed to call for config {}", config, e);
        } catch (Exception e) {
          LOG.error("Fatal error for config {}", config, e); // how should we handle that case?
        }
      }
    });
  }

  @Override
  public boolean canExecuteAnother() {
    return threadPool.getActiveCount() < threadPool.getMaximumPoolSize();
  }
}
