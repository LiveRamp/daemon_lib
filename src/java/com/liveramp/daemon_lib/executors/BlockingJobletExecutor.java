package com.liveramp.daemon_lib.executors;

import java.util.ArrayList;
import java.util.List;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class BlockingJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletFactory<T> jobletFactory;
  private final List<JobletCallback<T>> jobletCallbacks;

  public BlockingJobletExecutor(JobletFactory<T> jobletFactory, List<JobletCallback<T>> postExecutionCallbacks) {
    this.jobletFactory = jobletFactory;
    this.jobletCallbacks = postExecutionCallbacks;
  }

  public BlockingJobletExecutor(JobletFactory<T> jobletFactory) {
    this(jobletFactory, new ArrayList<JobletCallback<T>>());
  }

  @Override
  public void execute(T jobletConfig) throws DaemonException {
    Joblet joblet = jobletFactory.create(jobletConfig);
    try {
      joblet.run();
    } finally {
      for (JobletCallback<T> jobletCallback : jobletCallbacks) {
        jobletCallback.callback(jobletConfig);
      }
    }
  }

  @Override
  public boolean canExecuteAnother() {
    return true;
  }
}
