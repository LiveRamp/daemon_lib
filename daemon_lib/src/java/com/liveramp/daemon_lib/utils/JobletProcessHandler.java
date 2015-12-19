package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;
import com.liveramp.daemon_lib.tracking.JobletStatus;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private final JobletCallback<T> successCallback;
  private final JobletCallback<T> failureCallback;
  private final JobletConfigStorage<T> configStorage;
  private final JobletStatusManager jobletStatusManager;

  public JobletProcessHandler(JobletCallback<T> successCallback, JobletCallback<T> failureCallback, JobletConfigStorage<T> configStorage, JobletStatusManager jobletStatusManager) {
    this.successCallback = successCallback;
    this.failureCallback = failureCallback;
    this.configStorage = configStorage;
    this.jobletStatusManager = jobletStatusManager;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      final String identifier = watchedProcess.getMetadata().getIdentifier();
      T jobletConfig = configStorage.loadConfig(identifier);
      if (jobletStatusManager.exists(identifier)) {
        JobletStatus status = jobletStatusManager.getStatus(identifier);
        switch (status) {
          case DONE:
            successCallback.callback(jobletConfig);
            break;
          default:
            failureCallback.callback(jobletConfig);
            break;
        }

        jobletStatusManager.remove(identifier);
        configStorage.deleteConfig(identifier);
      }
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
