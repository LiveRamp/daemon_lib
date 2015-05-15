package com.liveramp.daemon_lib.joblet_executors;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.local.FsHelper;
import com.liveramp.daemon_lib.local.LocalProcessController;
import com.liveramp.daemon_lib.local.PsPidGetter;
import com.liveramp.daemon_lib.utils.ForkedJobletRunner;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;
import com.liveramp.daemon_lib.utils.JobletProcessHandler;

public class JobletExecutors {
  public static class Forked {
    private static final int DEFAULT_POLL_DELAY = 1000;

    public static <T extends JobletConfig> ForkedJobletExecutor<T> get(String tmpPath, int maxProcesses, JobletFactory<T> jobletFactory) throws IOException {
      File pidDir = new File(tmpPath, "pids");
      File configStoreDir = new File(tmpPath, "config_store");
      FileUtils.forceMkdir(pidDir);

      JobletConfigStorage<T> configStore = JobletConfigStorage.<T>production(configStoreDir.getPath());
      LocalProcessController<JobletConfigMetadata> processController = new LocalProcessController<JobletConfigMetadata>(
          new FsHelper(pidDir.getPath()),
          new JobletProcessHandler<T>(jobletFactory, configStore),
          new PsPidGetter(),
          DEFAULT_POLL_DELAY,
          new JobletConfigMetadata.Serializer()
      );

      Executors.newSingleThreadExecutor().submit(new ProcessControllerRunner(processController));

      @SuppressWarnings("unchecked")
      Class<? extends JobletFactory<? extends T>> jobletFactoryClass = (Class<? extends JobletFactory<? extends T>>)jobletFactory.getClass();

      return new ForkedJobletExecutor<T>(maxProcesses, jobletFactoryClass, configStore, processController, ForkedJobletRunner.production());
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
