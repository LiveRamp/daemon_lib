package com.liveramp.daemon_lib.executors;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.local.FsHelper;
import com.liveramp.daemon_lib.executors.processes.local.LocalProcessController;
import com.liveramp.daemon_lib.executors.processes.local.PsPidGetter;
import com.liveramp.daemon_lib.utils.ForkedJobletRunner;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;
import com.liveramp.daemon_lib.utils.JobletProcessHandler;

public class JobletExecutors {
  public static class Forked {
    private static final int DEFAULT_POLL_DELAY = 1000;

    public static <T extends JobletConfig> ForkedJobletExecutor<T> get(String tmpPath, int maxProcesses, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletCallbacks<T> jobletCallbacks) throws IOException, IllegalAccessException, InstantiationException {
      Preconditions.checkArgument(hasNoArgConstructor(jobletFactoryClass));

      File pidDir = new File(tmpPath, "pids");
      File configStoreDir = new File(tmpPath, "config_store");
      FileUtils.forceMkdir(pidDir);

      JobletConfigStorage<T> configStore = JobletConfigStorage.production(configStoreDir.getPath());
      LocalProcessController<JobletConfigMetadata> processController = new LocalProcessController<JobletConfigMetadata>(
          new FsHelper(pidDir.getPath()),
          new JobletProcessHandler<T>(jobletCallbacks, configStore),
          new PsPidGetter(),
          DEFAULT_POLL_DELAY,
          new JobletConfigMetadata.Serializer()
      );

      Executors.newSingleThreadExecutor().submit(new ProcessControllerRunner(processController));

      return new ForkedJobletExecutor<T>(maxProcesses, jobletFactoryClass, jobletCallbacks, configStore, processController, ForkedJobletRunner.production());
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

  private static class ProcessControllerRunner implements Runnable {
    private final LocalProcessController<JobletConfigMetadata> controller;

    private ProcessControllerRunner(LocalProcessController<JobletConfigMetadata> controller) {
      this.controller = controller;
    }

    @Override
    public void run() {
      controller.start();
    }
  }
}
