package com.liveramp.daemon_lib.utils;

import java.io.IOException;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.processes.ProcessUtil;
import com.liveramp.java_support.logging.LoggingHelper;

public class ForkedJobletRunner {
  private static final String JOBLET_RUNNER_SCRIPT = "src/java/com/liveramp/daemon_lib/utils/joblet_runner.sh";

  public static ForkedJobletRunner production() {
    return new ForkedJobletRunner();
  }

  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier) throws IOException {
    // TODO(asarkar): find a better way to escape the class name
    int pid = ProcessUtil.runCommand(JOBLET_RUNNER_SCRIPT, "\'" + jobletFactoryClass.getName() + "\'", configStore.getPath(), cofigIdentifier);

    return pid;
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    LoggingHelper.setLoggingProperties("forked-joblet-runner");

    String jobletFactoryClassName = args[0];
    String configStorePath = args[1];
    String id = args[2];

    JobletFactory factory = (JobletFactory)Class.forName(jobletFactoryClassName).newInstance();
    JobletConfig config = JobletConfigStorage.production(configStorePath).loadConfig(id);

    Joblet joblet = factory.create(config);
    joblet.run();
  }
}
