package com.liveramp.daemon_lib.utils;

import org.slf4j.Logger;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;
import com.liveramp.daemon_lib.tracking.JobletStatus;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;

import static org.slf4j.LoggerFactory.getLogger;

public class JobletProcessHandler<T extends JobletConfig> implements ProcessHandler<JobletConfigMetadata> {
  private static final Logger LOG = getLogger(JobletProcessHandler.class);

  private final JobletCallback<T> successCallback;
  private final JobletCallback<T> failureCallback;
  private final JobletConfigStorage<T> configStorage;
  private final JobletCallback<T> postExecutionCallback;
  private final JobletStatusManager jobletStatusManager;

  public JobletProcessHandler(JobletCallback<T> postExecutionCallback, JobletCallback<T> successCallback, JobletCallback<T> failureCallback, JobletConfigStorage<T> configStorage, JobletStatusManager jobletStatusManager) {
    this.postExecutionCallback = postExecutionCallback;
    this.successCallback = successCallback;
    this.failureCallback = failureCallback;
    this.configStorage = configStorage;
    this.jobletStatusManager = jobletStatusManager;
  }

  @Override
  public void onRemove(ProcessDefinition<JobletConfigMetadata> watchedProcess) throws DaemonException {
    try {
      JobletConfigMetadata metadata = watchedProcess.getMetadata();
      T jobletConfig = configStorage.loadConfig(metadata.getIdentifier());
      LOG.info("Calling onRemove callback for config {}", jobletConfig);
      postExecutionCallback.callback(jobletConfig);
      if (jobletStatusManager.exists(metadata.getIdentifier())) {
        JobletStatus status = jobletStatusManager.getStatus(metadata.getIdentifier());
        LOG.info("Joblet status: {}", status);
        switch (status) {
          case DONE:
            successCallback.callback(jobletConfig);
            break;
          default:
            failureCallback.callback(jobletConfig);
            break;
        }

        jobletStatusManager.remove(metadata.getIdentifier());
      }
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }
}
