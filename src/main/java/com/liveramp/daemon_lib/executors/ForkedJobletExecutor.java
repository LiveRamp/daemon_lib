package com.liveramp.daemon_lib.executors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.config.ExecutorConfig;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;
import com.liveramp.daemon_lib.executors.processes.MetadataFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.DefaultForkedExecutionCondition;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionCondition;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class ForkedJobletExecutor<T extends JobletConfig, M extends ProcessMetadata, Pid> implements JobletExecutor<T> {
  private final JobletConfigStorage<T> configStorage;
  private final ProcessController<M, Pid> processController;
  private final ProcessJobletRunner<Pid> jobletRunner;
  private final Class<? extends JobletFactory<? extends T>> jobletFactoryClass;
  private MetadataFactory<M> metadataFactory;
  private final Map<String, String> envVariables;
  private final String workingDir;
  private final JobletCallback<? super T> failureCallback;
  private final Supplier<Config> configSupplier;

  ForkedJobletExecutor(Class<? extends JobletFactory<? extends T>> jobletFactoryClass, JobletConfigStorage<T> configStorage, ProcessController<M, Pid> processController, ProcessJobletRunner<Pid> jobletRunner, MetadataFactory<M> metadataFactory, Map<String, String> envVariables, String workingDir, JobletCallback<? super T> failureCallback, Supplier<Config> configSupplier) {
    this.jobletFactoryClass = jobletFactoryClass;
    this.configStorage = configStorage;
    this.processController = processController;
    this.jobletRunner = jobletRunner;
    this.metadataFactory = metadataFactory;
    this.envVariables = envVariables;
    this.workingDir = workingDir;
    this.failureCallback = failureCallback;
    this.configSupplier = configSupplier;
  }

  @Override
  public void execute(T config) throws DaemonException {
    try {
      String identifier = configStorage.storeConfig(config);
      Pid pid = jobletRunner.run(jobletFactoryClass, configStorage, identifier, envVariables, workingDir);
      processController.registerProcess(pid, metadataFactory.createMetadata(identifier, jobletFactoryClass, configStorage, envVariables));
    } catch (Exception e) {
      failureCallback.callback(config);
      throw new DaemonException(e);
    }
  }

  @Override
  public ExecutionCondition getDefaultExecutionCondition() {
    return new DefaultForkedExecutionCondition(processController, configSupplier.get().numJoblets);
  }

  @Override
  public void shutdown() {

  }

  public static class Builder<S extends JobletConfig, M extends ProcessMetadata, Pid> {
    private static final int DEFAULT_MAX_PROCESSES = 1;

    private Class<? extends JobletFactory<? extends S>> jobletFactoryClass;
    private JobletConfigStorage<S> configStorage;
    private ProcessController<M, Pid> processController;
    private ProcessJobletRunner<Pid> jobletRunner;
    private Map<String, String> envVariables;
    private String workingDir;
    private JobletCallback<? super S> failureCallback;
    private MetadataFactory<M> metadataFactory;
    private Supplier<Config> executorConfigSupplier;

    public Builder(String workingDir, Class<? extends JobletFactory<? extends S>> jobletFactoryClass, JobletConfigStorage<S> configStorage, ProcessController<M, Pid> processController, MetadataFactory<M> metadataFactory, ProcessJobletRunner<Pid> jobletRunner, JobletCallback<? super S> failureCallback) {
      this.workingDir = workingDir;
      this.jobletFactoryClass = jobletFactoryClass;
      this.configStorage = configStorage;
      this.processController = processController;

      this.envVariables = new HashMap<>();
      this.jobletRunner = jobletRunner;
      this.failureCallback = failureCallback;
      this.metadataFactory = metadataFactory;
      this.executorConfigSupplier = () -> new Config(DEFAULT_MAX_PROCESSES);
    }

    public Builder<S, M, Pid> setMaxProcesses(int maxProcesses) {
      this.executorConfigSupplier = () -> new Config(maxProcesses);
      return this;
    }

    public Builder<S, M, Pid> setJobletFactoryClass(Class<? extends JobletFactory<? extends S>> jobletFactoryClass) {
      this.jobletFactoryClass = jobletFactoryClass;
      return this;
    }

    public Builder<S, M, Pid> setConfigStorage(JobletConfigStorage<S> configStorage) {
      this.configStorage = configStorage;
      return this;
    }

    public Builder<S, M, Pid> setProcessController(ProcessController<M, Pid> processController) {
      this.processController = processController;
      return this;
    }

    public Builder<S, M, Pid> setFailureCallback(JobletCallback<S> failureCallback) {
      this.failureCallback = failureCallback;
      return this;
    }

    public Builder<S, M, Pid> setJobletRunner(ProcessJobletRunner jobletRunner) {
      this.jobletRunner = jobletRunner;
      return this;
    }

    public Builder<S, M, Pid> putAllEnvVariables(Map<String, String> envVariables) {
      this.envVariables.putAll(envVariables);
      return this;
    }

    public Builder<S, M, Pid> putEnvVariable(String key, String value) {
      this.envVariables.put(key, value);
      return this;
    }

    public Builder<S, M, Pid> setWorkingDir(String workingDir) {
      this.workingDir = workingDir;
      return this;
    }

    public Builder<S, M, Pid> setMetadataFactory(MetadataFactory<M> metadataFactory) {
      this.metadataFactory = metadataFactory;
      return this;
    }

    public Builder<S, M, Pid> setExecutorConfigSupplier(Supplier<Config> forkedExecutorConfigSupplier) {
      this.executorConfigSupplier = forkedExecutorConfigSupplier;
      return this;
    }

    public ForkedJobletExecutor<S, M, Pid> build() throws IOException {
      return new ForkedJobletExecutor<>(jobletFactoryClass, configStorage, processController, jobletRunner, metadataFactory, envVariables, workingDir, failureCallback, executorConfigSupplier);
    }
  }

  public static class Config implements ExecutorConfig {
    public final int numJoblets;

    public Config(int numJoblets) {
      this.numJoblets = numJoblets;
    }
  }

}
