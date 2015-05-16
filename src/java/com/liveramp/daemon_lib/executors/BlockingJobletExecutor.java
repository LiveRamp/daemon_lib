package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.DaemonException;

public class BlockingJobletExecutor implements JobletExecutor {
  private final JobletFactory jobletFactory;

  public BlockingJobletExecutor(JobletFactory jobletFactory) {
    this.jobletFactory = jobletFactory;
  }

  @Override
  public void execute(JobletConfig jobletConfig) throws DaemonException {
    Joblet joblet = jobletFactory.create(jobletConfig);
    joblet.run();
  }

  @Override
  public boolean canExecuteAnother() {
    return true;
  }
}
