package com.liveramp.daemon_lib.executors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.liveramp.daemon_lib.DaemonNotifier;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunners;
import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.executors.processes.ProcessControllerException;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class ForkedJobletExecutor<T extends JobletConfig> implements JobletExecutor<T> {
  private final JobletConfigStorage<T> configStorage;
  private final ProcessController<JobletConfigMetadata> processController;
  private final ProcessJobletRunner jobletRunner;
  private final int maxProcesses;
  private final Class<? extends JobletFactory<? extends T>> jobletFactoryClass;
  private final Map<String, String> envVariables;
  private final String workingDir;

  protected ForkedJobletExecutor(int maxProcesses, Class<? extends JobletFactory<? extends T>> jobletFactoryClass, JobletConfigStorage<T> configStorage, ProcessController<JobletConfigMetadata> processController, ProcessJobletRunner jobletRunner, Map<String, String> envVariables, String workingDir) {
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

  public static class Builder<S extends JobletConfig> {
    private static final int DEFAULT_MAX_PROCESSES = 1;

    private int maxProcesses;
    private Class<? extends JobletFactory<? extends S>> jobletFactoryClass;
    private JobletConfigStorage<S> configStorage;
    private ProcessController<JobletConfigMetadata> processController;
    private ProcessJobletRunner jobletRunner;
    private Map<String, String> envVariables;
    private final DaemonNotifier notifier;
    private String workingDir;

    public Builder(DaemonNotifier notifier, String workingDir, Class<? extends JobletFactory<? extends S>> jobletFactoryClass, JobletConfigStorage<S> configStorage, ProcessController<JobletConfigMetadata> processController) {
      this.notifier = notifier;
      this.workingDir = workingDir;
      this.jobletFactoryClass = jobletFactoryClass;
      this.configStorage = configStorage;

      this.maxProcesses = DEFAULT_MAX_PROCESSES;
      this.envVariables = new HashMap<>();

      this.jobletRunner = ProcessJobletRunners.production();
      this.processController = processController;
    }

    public Builder<S> setMaxProcesses(int maxProcesses) {
      this.maxProcesses = maxProcesses;
      return this;
    }

    public Builder<S> setJobletFactoryClass(Class<? extends JobletFactory<? extends S>> jobletFactoryClass) {
      this.jobletFactoryClass = jobletFactoryClass;
      return this;
    }

    public Builder<S> setConfigStorage(JobletConfigStorage<S> configStorage) {
      this.configStorage = configStorage;
      return this;
    }

    public Builder<S> setProcessController(ProcessController<JobletConfigMetadata> processController) {
      this.processController = processController;
      return this;
    }

    public Builder<S> setJobletRunner(ProcessJobletRunner jobletRunner) {
      this.jobletRunner = jobletRunner;
      return this;
    }

    public Builder<S> putAllEnvVariables(Map<String, String> envVariables) {
      envVariables.putAll(envVariables);
      return this;
    }

    public Builder<S> putEnvVariable(String key, String value) {
      envVariables.put(key, value);
      return this;
    }

    public Builder<S> setWorkingDir(String workingDir) {
      this.workingDir = workingDir;
      return this;
    }

    public ForkedJobletExecutor<S> build() throws IOException {
      return new ForkedJobletExecutor<>(maxProcesses, jobletFactoryClass, configStorage, processController, jobletRunner, envVariables, workingDir);
    }
  }
}
