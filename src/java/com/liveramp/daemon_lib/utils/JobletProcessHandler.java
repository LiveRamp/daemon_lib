package com.liveramp.daemon_lib.utils;

import java.io.IOException;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;
import com.liveramp.daemon_lib.tracking.JobletStatus;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;

public class JobletProcessHandler<T extends JobletConfig, Pid> implements ProcessHandler<JobletConfigMetadata, Pid> {
  private final JobletCallback<? super T> successCallback;
  private final JobletCallback<? super T> failureCallback;
  private final JobletConfigStorage<T> configStorage;
  private final JobletStatusManager jobletStatusManager;

  public JobletProcessHandler(JobletCallback<? super T> successCallback, JobletCallback<? super T> failureCallback, JobletConfigStorage<T> configStorage, JobletStatusManager jobletStatusManager) {
    this.successCallback = successCallback;
    this.failureCallback = failureCallback;
    this.configStorage = configStorage;
    this.jobletStatusManager = jobletStatusManager;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata, Pid> watchedProcess) throws DaemonException {
    final String identifier = watchedProcess.getMetadata().getIdentifier();

    final T jobletConfig;
    try {
      jobletConfig = configStorage.loadConfig(identifier);
    } catch (IOException | ClassNotFoundException e) {
      throw new DaemonException(String.format("Error retrieving config with ID %s", identifier), e);
    }

    if (jobletStatusManager.exists(identifier)) {
      try {
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
      } catch (Exception e) {
        throw new DaemonException(String.format("Error processing config %s", jobletConfig), e);
      }
    }
  }
}
