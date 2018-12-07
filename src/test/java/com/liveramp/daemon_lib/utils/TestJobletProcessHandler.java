package com.liveramp.daemon_lib.utils;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;
import com.liveramp.daemon_lib.tracking.JobletStatus;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class TestJobletProcessHandler {
  private final String IDENTIFIER = "id";

  @Mock
  private JobletCallback successCallback;

  @Mock
  private JobletCallback failureCallback;

  @Mock
  private JobletConfigStorage jobletConfigStorage;

  @Mock
  private JobletStatusManager jobletStatusManager;

  @Mock
  private ProcessDefinition processDefinition;

  @Mock
  private ProcessMetadata processMetadata;

  @Mock
  private JobletConfig jobletConfig;

  private JobletProcessHandler jobletProcessHandler;

  @Before
  public void setup() throws IOException, ClassNotFoundException {
    when(processDefinition.getMetadata()).thenReturn(processMetadata);
    when(processMetadata.getIdentifier()).thenReturn(IDENTIFIER);

    when(jobletConfigStorage.loadConfig(IDENTIFIER)).thenReturn(jobletConfig);

    when(jobletStatusManager.exists(IDENTIFIER)).thenReturn(true);

    jobletProcessHandler = new JobletProcessHandler<>(successCallback, failureCallback, jobletConfigStorage, jobletStatusManager);
  }


  @Test
  public void testOnRemoveAttemptsSuccessCallbackWhenStatusIsDone() throws DaemonException {
    when(jobletStatusManager.getStatus(IDENTIFIER)).thenReturn(JobletStatus.DONE);
    jobletProcessHandler.onRemove(processDefinition);
    verify(successCallback, times(1)).callback(jobletConfig);
  }

  @Test
  public void testOnRemoveAttemptsFailureCallbackWhenStatusIsInProgress() throws DaemonException {
    when(jobletStatusManager.getStatus(IDENTIFIER)).thenReturn(JobletStatus.IN_PROGRESS);
    jobletProcessHandler.onRemove(processDefinition);
    verify(failureCallback, times(1)).callback(jobletConfig);
  }

  @Test
  public void testOnRemoveAttemptsFailureCallbackOnException() throws DaemonException {
    when(jobletStatusManager.getStatus(IDENTIFIER)).thenThrow(new IllegalArgumentException());
    jobletProcessHandler.onRemove(processDefinition);
    verify(failureCallback, times(1)).callback(jobletConfig);
  }

  @Test
  public void testOnRemoveAttemptsFailureCallbackOnExceptionAndStillFails() throws DaemonException {
    when(jobletStatusManager.getStatus(IDENTIFIER)).thenThrow(new IllegalArgumentException());
    doThrow(new RuntimeException()).when(failureCallback).callback(jobletConfig);
    try {
      jobletProcessHandler.onRemove(processDefinition);
      Assert.fail();
    } catch (DaemonException ignored) {

    }
  }

}
