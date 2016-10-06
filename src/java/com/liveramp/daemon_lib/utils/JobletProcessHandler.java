package com.liveramp.daemon_lib.utils;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;
import com.liveramp.daemon_lib.executors.processes.local.ProcessHandler;
import com.liveramp.daemon_lib.tracking.JobletStatus;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;

public class JobletProcessHandler<T extends JobletConfig, Pid, M extends ProcessMetadata> implements ProcessHandler<M, Pid> {
  private final JobletCallback<? super T> successCallback;
  private final JobletCallback<? super T> failureCallback;
  private final JobletConfigStorage<T> configStorage;
  private final JobletStatusManager jobletStatusManager;

  private static Logger LOG = LoggerFactory.getLogger(JobletProcessHandler.class);

  public JobletProcessHandler(JobletCallback<? super T> successCallback, JobletCallback<? super T> failureCallback, JobletConfigStorage<T> configStorage, JobletStatusManager jobletStatusManager) {
    this.successCallback = successCallback;
    this.failureCallback = failureCallback;
    this.configStorage = configStorage;
    this.jobletStatusManager = jobletStatusManager;
  }

  @Override
  public void onRemove(ProcessDefinition<M, Pid> watchedProcess) throws DaemonException {
    LOG.info("Removing process with PID: "+watchedProcess.getPid());
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
            LOG.info("Process succeeded - PID: "+watchedProcess.getPid());
            successCallback.callback(jobletConfig);
            break;
          default:
            LOG.info("Process failed - PID: "+watchedProcess.getPid());
            failureCallback.callback(jobletConfig);
            break;
        }

        jobletStatusManager.remove(identifier);
        configStorage.deleteConfig(identifier);
      } catch (Exception e) {
        throw new DaemonException(String.format("Error processing config %s", jobletConfig), e);
      }
    }else{
      LOG.info("No Managed Status Found - PID: "+watchedProcess.getPid());
    }
  }
}
