package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Joiner;
import org.slf4j.Logger;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessUtil;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

import static org.slf4j.LoggerFactory.getLogger;

public class JarBasedProcessJobletRunner implements ProcessJobletRunner<Integer> {
  private static final Logger LOG = getLogger(JarBasedProcessJobletRunner.class);

  private final String executableCommand;
  private final String jarPath;

  private JarBasedProcessJobletRunner(String executableCommand, String jarPath) {
    this.executableCommand = executableCommand;
    this.jarPath = jarPath;
  }

  @Override
  public Integer run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass,
                 JobletConfigStorage configStore,
                 String cofigIdentifier,
                 Map<String, String> envVariables,
                 String workingDir) throws IOException, ClassNotFoundException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            executableCommand,
            "-cp",
            jarPath,
            ForkedJobletRunner.class.getName(),
            ForkedJobletRunner.quote(jobletFactoryClass.getName()),
            configStore.getPath(),
            workingDir,
            cofigIdentifier
        );

    processBuilder.environment().putAll(envVariables);

    LOG.info("Running command: {}", Joiner.on(' ').join(processBuilder.command()));

    return ProcessUtil.run(processBuilder);
  }

  @Override
  public void shutdown() {

  }

  public static class Builder {
    private static final String DEFAULT_COMMAND = "java";

    private String jarPath;
    private String executableCommand;

    private Builder(String jarPath) {
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

  public static Builder builder(String jarPath) {
    return new Builder(jarPath);
  }
}
