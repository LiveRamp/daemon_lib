package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class BlockingJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletFactory<T> jobletFactory;

  public BlockingJobletExecutor(JobletFactory<T> jobletFactory) {
    this.jobletFactory = jobletFactory;
  }

  @Override
  public void execute(T jobletConfig) throws DaemonException {
    Joblet joblet = jobletFactory.create(jobletConfig);
    try {
      joblet.run();
    } catch (DaemonException e) {
      joblet.afterExecution();
    }
  }

  @Override
  public boolean canExecuteAnother() {
    return true;
  }
}
