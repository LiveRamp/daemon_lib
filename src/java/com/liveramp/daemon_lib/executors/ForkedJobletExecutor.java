package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.ForkedJobletRunner;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class ForkedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletCallbacks<T> jobletCallbacks;
  private final JobletConfigStorage<T> configStorage;
  private final ProcessController<JobletConfigMetadata> processController;
  private final ForkedJobletRunner jobletRunner;
  private final int maxProcesses;
  private final Class<? extends JobletFactory<? extends T>> jobletFactoryClass;

  public ForkedJobletExecutor(int maxProcesses, Class<? extends JobletFactory<? extends T>> jobletFactoryClass, JobletCallbacks<T> jobletCallbacks, JobletConfigStorage<T> configStorage, ProcessController<JobletConfigMetadata> processController, ForkedJobletRunner jobletRunner) {
    this.maxProcesses = maxProcesses;
    this.jobletFactoryClass = jobletFactoryClass;
    this.jobletCallbacks = jobletCallbacks;
    this.configStorage = configStorage;
    this.processController = processController;
    this.jobletRunner = jobletRunner;
  }

  @Override
  public void execute(T config) throws DaemonException {
    try {
      String identifier = configStorage.storeConfig(config);
      jobletCallbacks.before(config);
      int pid = jobletRunner.run(jobletFactoryClass, configStorage, identifier);
      processController.registerProcess(pid, new JobletConfigMetadata(identifier));
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }

  @Override
  public boolean canExecuteAnother() {
    return processController.getProcesses().size() < maxProcesses;
  }
}
