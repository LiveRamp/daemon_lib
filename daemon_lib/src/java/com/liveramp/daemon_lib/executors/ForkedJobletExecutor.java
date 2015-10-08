package com.liveramp.daemon_lib.executors;

import java.util.Map;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.executors.processes.ProcessControllerException;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.ForkedJobletRunner;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class ForkedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletConfigStorage<T> configStorage;
  private final ProcessController<JobletConfigMetadata> processController;
  private final ForkedJobletRunner jobletRunner;
  private final int maxProcesses;
  private final Class<? extends JobletFactory<? extends T>> jobletFactoryClass;
  private final Map<String, String> envVariables;
  private final String workingDir;

  public ForkedJobletExecutor(int maxProcesses, Class<? extends JobletFactory<? extends T>> jobletFactoryClass, JobletConfigStorage<T> configStorage, ProcessController<JobletConfigMetadata> processController, ForkedJobletRunner jobletRunner, Map<String, String> envVariables, String workingDir) {
    this.maxProcesses = maxProcesses;
    this.jobletFactoryClass = jobletFactoryClass;
    this.configStorage = configStorage;
    this.processController = processController;
    this.jobletRunner = jobletRunner;
    this.envVariables = envVariables;
    this.workingDir = workingDir;
  }

  @Override
  public void execute(T config) throws DaemonException {
    try {
      String identifier = configStorage.storeConfig(config);
      int pid = jobletRunner.run(jobletFactoryClass, configStorage, identifier, envVariables, workingDir);
      processController.registerProcess(pid, new JobletConfigMetadata(identifier));
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }

  @Override
  public boolean canExecuteAnother() {
    try {
      return processController.getProcesses().size() < maxProcesses;
    } catch (ProcessControllerException e) {
      return false;
    }
  }

  @Override
  public void shutdown() {

  }
}
