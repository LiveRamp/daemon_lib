package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private final JobletConfigStorage<T> configStorage;
  private final JobletCallback<T> postExecutionCallback;

  public JobletProcessHandler(JobletCallback<T> postExecutionCallback, JobletConfigStorage<T> configStorage) {
    this.postExecutionCallback = postExecutionCallback;
    this.configStorage = configStorage;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      postExecutionCallback.callback(jobletConfig);
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
