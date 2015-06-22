package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class BlockingJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletFactory<T> jobletFactory;
  private final JobletCallbacks<T> jobletCallbacks;

  public BlockingJobletExecutor(JobletFactory<T> jobletFactory, JobletCallbacks<T> jobletCallbacks) {
    this.jobletFactory = jobletFactory;
    this.jobletCallbacks = jobletCallbacks;
  }

  public BlockingJobletExecutor(JobletFactory<T> jobletFactory) {
    this(jobletFactory, new JobletCallbacks.None<T>());
  }

  @Override
  public void execute(T jobletConfig) throws DaemonException {
    jobletCallbacks.before(jobletConfig);
    Joblet joblet = jobletFactory.create(jobletConfig);
    try {
      joblet.run();
    } finally {
      jobletCallbacks.after(jobletConfig);
    }
  }

  @Override
  public boolean canExecuteAnother() {
    return true;
  }
}
