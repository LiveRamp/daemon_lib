package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private final JobletConfigStorage<T> configStorage;
  private final JobletCallbacks<T> callbacks;

  public JobletProcessHandler(JobletCallbacks<T> callbacks, JobletConfigStorage<T> configStorage) {
    this.callbacks = callbacks;
    this.configStorage = configStorage;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      callbacks.after(jobletConfig);
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
