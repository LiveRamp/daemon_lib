package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;
import com.liveramp.daemon_lib.executors.processes.ProcessUtil;
import com.liveramp.daemon_lib.tracking.DefaultJobletStatusManager;
import com.liveramp.java_support.logging.LoggingHelper;

public class ForkedJobletRunner implements ProcessJobletRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ForkedJobletRunner.class);

  private static final String JOBLET_RUNNER_SCRIPT = "bin/joblet_runner.sh";
  private static final String JOBLET_RUNNER_SCRIPT_SOURCE = "com/liveramp/daemon_lib/utils/joblet_runner.txt";

  @Override
  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier, Map<String, String> envVariables, String workingDir) throws IOException {
    prepareScript();

    ProcessBuilder processBuilder = new ProcessBuilder(JOBLET_RUNNER_SCRIPT, quote(jobletFactoryClass.getName()), configStore.getPath(), workingDir, cofigIdentifier);
    processBuilder.environment().putAll(envVariables);
    int pid = ProcessUtil.run(processBuilder);

    return pid;
  }

  private static void prepareScript() throws IOException {
    File productionScript = new File(JOBLET_RUNNER_SCRIPT);
    if (!productionScript.exists()) {
      LOG.info("joblet_runner script doesn't exist - creating it now");
      InputStream scriptResourceInput = ForkedJobletRunner.class.getClassLoader().getResourceAsStream(JOBLET_RUNNER_SCRIPT_SOURCE);

      FileUtils.forceMkdir(productionScript.getParentFile());
      FileOutputStream scriptProductionOutput = new FileOutputStream(JOBLET_RUNNER_SCRIPT);

      ByteStreams.copy(scriptResourceInput, scriptProductionOutput);

      scriptResourceInput.close();
      scriptProductionOutput.close();

      productionScript.setExecutable(true);
    }
  }

  private static String quote(String s) {
    return "'" + s + "'";
  }

  private static String unquote(String s) {
    if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
      return s.substring(1, s.length() - 1);
    } else {
      return s;
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, DaemonException {
    LoggingHelper.setLoggingProperties("forked-joblet-runner/forked-joblet-runner");

    String jobletFactoryClassName = unquote(args[0]);
    String configStorePath = args[1];
    String daemonWorkingDir = args[2];
    String id = args[3];

    JobletFactory factory = (JobletFactory)Class.forName(jobletFactoryClassName).newInstance();
    JobletConfig config = JobletConfigStorage.production(configStorePath).loadConfig(id);
    DefaultJobletStatusManager jobletStatusManager = new DefaultJobletStatusManager(daemonWorkingDir);

    Joblet joblet = factory.create(config);
    jobletStatusManager.start(id);
    joblet.run();
    jobletStatusManager.complete(id);
  }
}
