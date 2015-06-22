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
  private final int maxActiveThreads;
  private final JobletFactory<T> jobletFactory;
  private final JobletCallbacks<T> jobletCallbacks;

  public ThreadedJobletExecutor(ThreadPoolExecutor threadPool, int maxActiveThreads, JobletFactory<T> jobletFactory, JobletCallbacks<T> jobletCallbacks) {
    this.threadPool = threadPool;
    this.maxActiveThreads = maxActiveThreads;
    this.jobletFactory = jobletFactory;
    this.jobletCallbacks = jobletCallbacks;
  }

  @Override
  public void execute(final T config) throws DaemonException {
    jobletCallbacks.before(config);
    threadPool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Joblet joblet = jobletFactory.create(config);
          joblet.run();
        } catch (DaemonException e) {
          LOG.error("Failed to create joblet for config {}", config, e);
        } finally {
          try {
            jobletCallbacks.after(config);
          } catch (DaemonException e) {
            LOG.error("Failed to call after for config {}", config, e);
          }
        }
      }
    });
  }

  @Override
  public boolean canExecuteAnother() {
    return threadPool.getActiveCount() < maxActiveThreads;
  }
}
