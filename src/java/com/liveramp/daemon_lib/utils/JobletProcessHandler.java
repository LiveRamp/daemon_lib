package com.liveramp.daemon_lib.utils;

import java.util.List;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private final JobletConfigStorage<T> configStorage;
  private final List<JobletCallback<T>> postExecutionCallbacks;

  public JobletProcessHandler(List<JobletCallback<T>> postExecutionCallbacks, JobletConfigStorage<T> configStorage) {
    this.postExecutionCallbacks = postExecutionCallbacks;
    this.configStorage = configStorage;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      for (JobletCallback<T> callback : postExecutionCallbacks) {
        callback.callback(jobletConfig);
      }
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
