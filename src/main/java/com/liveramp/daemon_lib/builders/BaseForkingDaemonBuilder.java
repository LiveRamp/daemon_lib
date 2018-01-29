package com.liveramp.daemon_lib.builders;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.built_in.CompositeDeserializer;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.JobletExecutors;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public abstract class BaseForkingDaemonBuilder<T extends JobletConfig, E extends BaseForkingDaemonBuilder<T, E>> extends BaseDaemonBuilder<T, E> {

  private final String workingDir;
  private final Class<? extends JobletFactory<T>> jobletFactoryClass;
  private int maxProcesses;
  private Map<String, String> envVariables;
  private ProcessJobletRunner jobletRunner;

  private static final int DEFAULT_MAX_PROCESSES = 1;
  private static final Map<String, String> DEFAULT_ENV_VARS = Maps.newHashMap();
  private Function<? super T, byte[]> serializer;
  private List<Function<byte[], ? extends T>> customDeserializers;

  protected BaseForkingDaemonBuilder(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, ProcessJobletRunner jobletRunner) {
    super(identifier, configProducer);
    this.workingDir = workingDir;
    this.jobletFactoryClass = jobletFactoryClass;
    this.jobletRunner = jobletRunner;

    maxProcesses = DEFAULT_MAX_PROCESSES;
    envVariables = DEFAULT_ENV_VARS;
    serializer = JobletConfigStorage.DEFAULT_SERIALIZER;
    customDeserializers = Lists.newArrayList();
  }

  public E setMaxProcesses(int maxProcesses) {
    this.maxProcesses = maxProcesses;
    return self();
  }

  public E addToEnvironmentVariables(String envVar, String value) {
    this.envVariables.put(envVar, value);
    return self();
  }

  public E setConfigStorageSerializer(Function<T, byte[]> serializer) {
    this.serializer = serializer;
    return self();
  }

  public E addToConfigStorageDeserializers(Function<byte[], ? extends T> deserializer) {
    this.customDeserializers.add(deserializer);
    return self();
  }

  @NotNull
  @Override
  protected JobletExecutor<T> getExecutor() throws IllegalAccessException, IOException, InstantiationException {
    List<Function<byte[], ? extends T>> deserializersWithDefault = Lists.newArrayList(customDeserializers);
    deserializersWithDefault.add(JobletConfigStorage.getDefaultDeserializer());
    final String tmpPath = new File(workingDir, identifier).getPath();
    return JobletExecutors.Forked.get(notifier, tmpPath, maxProcesses, jobletFactoryClass, envVariables, successCallback, failureCallback, jobletRunner, serializer, new CompositeDeserializer<>(deserializersWithDefault));
  }

}
