package com.liveramp.daemon_lib.executors.forking;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Joiner;
import org.slf4j.Logger;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessUtil;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

import static org.slf4j.LoggerFactory.getLogger;

public class JarBasedProcessJobletRunner implements ProcessJobletRunner {
  private static final Logger LOG = getLogger(JarBasedProcessJobletRunner.class);

  private final String executableCommand;
  private final String jarPath;

  private JarBasedProcessJobletRunner(String executableCommand, String jarPath) {
    this.executableCommand = executableCommand;
    this.jarPath = jarPath;
  }

  @Override
  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier, Map<String, String> envVariables, String workingDir) throws IOException, ClassNotFoundException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(executableCommand,
            jarPath,
            ForkedJobletRunner.class.getName(),
            ForkedJobletRunner.quote(jobletFactoryClass.getName()),
            configStore.getPath(),
            workingDir,
            cofigIdentifier);

    processBuilder.environment().putAll(envVariables);

    File out = new File("log/process_runner.out");
    processBuilder.redirectOutput(out);
    processBuilder.redirectError(out);

    LOG.info("Running command: {}", Joiner.on(' ').join(processBuilder.command()));

    int pid = ProcessUtil.run(processBuilder);

    return pid;
  }

  public static class Builder {
    private static final String DEFAULT_COMMAND = "java";

    private String jarPath;
    private String executableCommand;

    public Builder(String jarPath) {
      this.jarPath = jarPath;
      this.executableCommand = DEFAULT_COMMAND;
    }

    public Builder setJarPath(String jarPath) {
      this.jarPath = jarPath;

      return this;
    }

    public Builder setExecutableCommand(String executableCommand) {
      this.executableCommand = executableCommand;

      return this;
    }

    public JarBasedProcessJobletRunner build() {
      return new JarBasedProcessJobletRunner(executableCommand, jarPath);
    }
  }
}
