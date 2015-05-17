package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessUtil;
import com.liveramp.java_support.logging.LoggingHelper;

public class ForkedJobletRunner {
  private static final String JOBLET_RUNNER_SCRIPT = "bin/joblet_runner.sh";
  private static final String JOBLET_RUNNER_SCRIPT_SOURCE = "com/liveramp/daemon_lib/utils/joblet_runner.txt";

  public static ForkedJobletRunner production() {
    return new ForkedJobletRunner();
  }

  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier) throws IOException {
    prepareScript();

    int pid = ProcessUtil.runCommand(JOBLET_RUNNER_SCRIPT, escape(jobletFactoryClass.getName()), configStore.getPath(), cofigIdentifier);

    return pid;
  }

  private static String escape(String str) {
    // support nested static classes
    return StringUtils.replace(str, "$", "\\$");
  }

  private static void prepareScript() throws IOException {
    File productionScript = new File(JOBLET_RUNNER_SCRIPT);
    if (!productionScript.exists()) {
      InputStream scriptResourceInput = ForkedJobletRunner.class.getClassLoader().getResourceAsStream(JOBLET_RUNNER_SCRIPT_SOURCE);

      FileUtils.forceMkdir(productionScript.getParentFile());
      FileOutputStream scriptProductionOutput = new FileOutputStream(JOBLET_RUNNER_SCRIPT);

      ByteStreams.copy(scriptResourceInput, scriptProductionOutput);

      scriptResourceInput.close();
      scriptProductionOutput.close();

      productionScript.setExecutable(true);
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, DaemonException {
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
