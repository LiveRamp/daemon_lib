package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private final JobletFactory<T> jobletFactory;
  private final JobletConfigStorage<T> configStorage;

  public JobletProcessHandler(JobletFactory<T> jobletFactory, JobletConfigStorage<T> configStorage) {
    this.jobletFactory = jobletFactory;
    this.configStorage = configStorage;
  }

  @Override
  public void onAdd(ProcessDefinition<JobletConfigMetadata> watchedProcess) {
    // Do nothing for now
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      Joblet joblet = jobletFactory.create(jobletConfig);
      joblet.onComplete();
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
