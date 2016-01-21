package com.liveramp.daemon_lib.executors;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;

import com.liveramp.daemon_lib.DaemonNotifier;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.local.FsHelper;
import com.liveramp.daemon_lib.executors.processes.local.LocalProcessController;
import com.liveramp.daemon_lib.executors.processes.local.PsPidGetter;
import com.liveramp.daemon_lib.tracking.DefaultJobletStatusManager;
import com.liveramp.daemon_lib.tracking.JobletStatusManager;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;
import com.liveramp.daemon_lib.utils.JobletProcessHandler;

public class JobletExecutors {

  public static class Blocking {

    public static <T extends JobletConfig> BlockingJobletExecutor<T> get(JobletFactory<T> jobletFactory, JobletCallback<T> successCallback, JobletCallback<T> failureCallback) throws IllegalAccessException, InstantiationException {
      return new BlockingJobletExecutor<>(jobletFactory, successCallback, failureCallback);
    }
  }

  public static class Forked {
    private static final int DEFAULT_POLL_DELAY = 1000;

    public static <T extends JobletConfig> ForkedJobletExecutor<T> get(DaemonNotifier notifier, String tmpPath, int maxProcesses, Class<? extends JobletFactory<T>> jobletFactoryClass, Map<String, String> envVariables, JobletCallback<T> successCallback, JobletCallback<T> failureCallback) throws IOException, IllegalAccessException, InstantiationException {
      Preconditions.checkArgument(hasNoArgConstructor(jobletFactoryClass), String.format("Class %s has no accessible no-arg constructor", jobletFactoryClass.getName()));

      File pidDir = new File(tmpPath, "pids");
      File configStoreDir = new File(tmpPath, "config_store");
      FileUtils.forceMkdir(pidDir);

      JobletConfigStorage<T> configStore = JobletConfigStorage.production(configStoreDir.getPath());
      JobletStatusManager jobletStatusManager = new DefaultJobletStatusManager(tmpPath);
      LocalProcessController<JobletConfigMetadata> processController = new LocalProcessController<>(
          notifier,
          new FsHelper(pidDir.getPath()),
          new JobletProcessHandler<>(successCallback, failureCallback, configStore, jobletStatusManager),
          new PsPidGetter(),
          DEFAULT_POLL_DELAY,
          new JobletConfigMetadata.Serializer()
      );

      return new ForkedJobletExecutor.Builder<>(tmpPath, jobletFactoryClass, configStore, processController)
          .setMaxProcesses(maxProcesses)
          .putAllEnvVariables(envVariables)
          .build();
    }
  }

  public static class Threaded {
    @Deprecated
    public static <T extends JobletConfig> ThreadedJobletExecutor<T> get(int maxActiveJoblets, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallback<T> successCallbacks, JobletCallback<T> failureCallbacks) throws IllegalAccessException, InstantiationException {
      return get(maxActiveJoblets, jobletFactoryClass.newInstance(), successCallbacks, failureCallbacks);
    }

    public static <T extends JobletConfig> ThreadedJobletExecutor<T> get(int maxActiveJoblets, JobletFactory<T> jobletFactory, JobletCallback<T> successCallbacks, JobletCallback<T> failureCallbacks) throws IllegalAccessException, InstantiationException {
      Preconditions.checkNotNull(jobletFactory);
      Preconditions.checkArgument(maxActiveJoblets > 0);

      ThreadPoolExecutor threadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(
          maxActiveJoblets,
          new ThreadFactoryBuilder().setNameFormat("joblet-executor-%d").build()
      );

      return new ThreadedJobletExecutor<>(threadPool, jobletFactory, successCallbacks, failureCallbacks);
    }
  }

  private static boolean hasNoArgConstructor(Class klass) {
    for (Constructor constructor : klass.getConstructors()) {
      if (constructor.getParameterTypes().length == 0) {
        return true;
      }
    }

    return false;
  }

}
