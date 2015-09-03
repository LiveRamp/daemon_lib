package com.liveramp.daemon_lib.executors;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class ThreadedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadedJobletExecutor.class);

  private final ThreadPoolExecutor threadPool;
  private final int maxActiveThreads;
  private final JobletFactory<T> jobletFactory;
  private final JobletCallback<T> postExecutionCallback;
  private final ConcurrentLinkedQueue<Exception> uncheckedExceptionsFromTasks;

  public ThreadedJobletExecutor(ThreadPoolExecutor threadPool, int maxActiveThreads, JobletFactory<T> jobletFactory, JobletCallback<T> postExecutionCallback) {
    this.threadPool = threadPool;
    this.maxActiveThreads = maxActiveThreads;
    this.jobletFactory = jobletFactory;
    this.postExecutionCallback = postExecutionCallback;
    this.uncheckedExceptionsFromTasks = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void execute(final T config) throws DaemonException {
    if (!uncheckedExceptionsFromTasks.isEmpty()) {
      throw new RuntimeException(uncheckedExceptionsFromTasks.poll());
    }

    threadPool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Joblet joblet = jobletFactory.create(config);
          joblet.run();
        } catch (DaemonException e) {
          LOG.error("Failed to run joblet for config {}", config, e);
        } catch (Exception e) {
          LOG.error("Fatal error for config {}", config, e);
          uncheckedExceptionsFromTasks.add(e);
        } finally {
          try {
            postExecutionCallback.callback(config);
          } catch (DaemonException e) {
            LOG.error("Failed to call after for config {}", config, e);
          } catch (Exception e) {
            LOG.error("Fatal error in call after config {}", config, e);
            uncheckedExceptionsFromTasks.add(e);
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
