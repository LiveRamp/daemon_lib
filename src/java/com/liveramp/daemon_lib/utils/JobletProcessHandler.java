package com.liveramp.daemon_lib.utils;

import java.io.IOException;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.local.ProcessHandler;
import com.liveramp.daemon_lib.processes.ProcessDefinition;

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
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      Joblet joblet = jobletFactory.create(jobletConfig);
      joblet.onComplete();
    } catch (IOException e) {
      // TODO(asarkar): figure this out
    }
  }
}
