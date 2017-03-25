package com.liveramp.daemon_lib.executors;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;

import com.liveramp.daemon_lib.DaemonNotifier;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;
import com.liveramp.daemon_lib.executors.processes.local.FsHelper;
import com.liveramp.daemon_lib.executors.processes.local.JobletConfigMetadataFactory;
import com.liveramp.daemon_lib.executors.processes.local.LocalMetadataProcessController;
import com.liveramp.daemon_lib.executors.processes.local.LocalProcessPidProcessor;
import com.liveramp.daemon_lib.executors.processes.local.PsRunningProcessGetter;
import com.liveramp.daemon_lib.tracking.DefaultJobletStatusManager;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;
import com.liveramp.daemon_lib.utils.JobletProcessHandler;

public class JobletExecutors {

  public static class Blocking {

    public static <T extends JobletConfig> BlockingJobletExecutor<T> get(JobletFactory<T> jobletFactory, JobletCallback<? super T> successCallback, JobletCallback<? super T> failureCallback) throws IllegalAccessException, InstantiationException {
      return new BlockingJobletExecutor<>(jobletFactory, successCallback, failureCallback);
    }
  }

  public static class Forked {
    private static final int DEFAULT_POLL_DELAY = 1000;

    public static <T extends JobletConfig> ForkedJobletExecutor<T,JobletConfigMetadata,Integer> get(DaemonNotifier notifier, String tmpPath, Class<? extends JobletFactory<T>> jobletFactoryClass, Map<String, String> envVariables, JobletCallback<? super T> successCallback, JobletCallback<? super T> failureCallback, ProcessJobletRunner<Integer> jobletRunner, Supplier<ForkedJobletExecutor.Config> executorConfigSupplier) throws IOException, IllegalAccessException, InstantiationException {
      Preconditions.checkArgument(hasNoArgConstructor(jobletFactoryClass), String.format("Class %s has no accessible no-arg constructor", jobletFactoryClass.getName()));

      File pidDir = new File(tmpPath, "pids");
      File configStoreDir = new File(tmpPath, "config_store");
      FileUtils.forceMkdir(pidDir);

      JobletConfigStorage<T> configStore = JobletConfigStorage.production(configStoreDir.getPath());
      JobletStatusManager jobletStatusManager = new DefaultJobletStatusManager(tmpPath);
      LocalMetadataProcessController<JobletConfigMetadata, Integer> processController = new LocalMetadataProcessController<>(
          notifier,
          new FsHelper(pidDir.getPath()),
          new LocalProcessPidProcessor(),
          new JobletProcessHandler<T, Integer, JobletConfigMetadata>(successCallback, failureCallback, configStore, jobletStatusManager),
          new PsRunningProcessGetter(),
          DEFAULT_POLL_DELAY,
          new JobletConfigMetadata.Serializer()
      );

      JobletConfigMetadataFactory metadataFactory = new JobletConfigMetadataFactory();

      return new ForkedJobletExecutor.Builder<>(tmpPath, jobletFactoryClass, configStore, processController, metadataFactory, jobletRunner, failureCallback)
          .setExecutorConfigSupplier(executorConfigSupplier)
          .putAllEnvVariables(envVariables)
          .build();
    }
  }

  public static class Threaded {
    @Deprecated
    public static <T extends JobletConfig> ThreadedJobletExecutor<T> get(int maxActiveJoblets, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallback<T> successCallbacks, JobletCallback<T> failureCallbacks) throws IllegalAccessException, InstantiationException {
      return get(jobletFactoryClass.newInstance(), successCallbacks, failureCallbacks, () -> new ThreadedJobletExecutor.Config(maxActiveJoblets));
    }

    public static <T extends JobletConfig> ThreadedJobletExecutor<T> get(JobletFactory<T> jobletFactory, JobletCallback<T> successCallbacks, JobletCallback<T> failureCallbacks, Supplier<ThreadedJobletExecutor.Config> threadedExecutorConfigSupplier) throws IllegalAccessException, InstantiationException {
      Preconditions.checkNotNull(jobletFactory);

      ThreadPoolExecutor threadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(
          threadedExecutorConfigSupplier.get().numJoblets,
          new ThreadFactoryBuilder().setNameFormat("joblet-executor-%d").build()
      );

      return new ThreadedJobletExecutor<>(threadPool, jobletFactory, successCallbacks, failureCallbacks, threadedExecutorConfigSupplier);
    }
  }

  public static boolean hasNoArgConstructor(Class klass) {
    for (Constructor constructor : klass.getConstructors()) {
      if (constructor.getParameterTypes().length == 0) {
        return true;
      }
    }

    return false;
  }

}
