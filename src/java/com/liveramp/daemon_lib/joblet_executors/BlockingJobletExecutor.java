package com.liveramp.daemon_lib.joblet_executors;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletExecutor;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletFactory;

public class BlockingJobletExecutor implements JobletExecutor {
  private final JobletFactory jobletFactory;

  public BlockingJobletExecutor(JobletFactory jobletFactory) {
    this.jobletFactory = jobletFactory;
  }

  @Override
  public void execute(JobletConfig jobletConfig) {
    Joblet joblet = jobletFactory.create(jobletConfig);
    joblet.run();
  }

  @Override
  public boolean canExecuteAnother() {
    return true;
  }
}
