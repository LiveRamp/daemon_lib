package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
  private final List<String> jvmOptions;

  private JarBasedProcessJobletRunner(String executableCommand, String jarPath, List<String> jvmOptions) {
    this.executableCommand = executableCommand;
    this.jarPath = jarPath;
    this.jvmOptions = jvmOptions;
  }

  @Override
  public Integer run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass,
                     JobletConfigStorage configStore,
                     String cofigIdentifier,
                     Map<String, String> envVariables,
                     String workingDir) throws IOException, ClassNotFoundException {
    ImmutableList.Builder<String> commandListBuilder = ImmutableList.builder();
    commandListBuilder.add(executableCommand);
    commandListBuilder.addAll(jvmOptions);
    commandListBuilder.addAll(Arrays.asList(
        "-cp",
        jarPath,
        ForkedJobletRunner.class.getName(),
        ForkedJobletRunner.quote(jobletFactoryClass.getName()),
        configStore.getPath(),
        workingDir,
        cofigIdentifier
    ));
    ProcessBuilder processBuilder = new ProcessBuilder(commandListBuilder.build());

    processBuilder.environment().putAll(envVariables);

    LOG.info("Running command: {}", Joiner.on(' ').join(processBuilder.command()));

    return ProcessUtil.run(processBuilder);
  }

  public static class Builder {
    private static final String DEFAULT_COMMAND = "java";

    private String jarPath;
    private String executableCommand;
    private List<String> jvmOptions;

    private Builder(String jarPath) {
      this.jarPath = jarPath;
      this.executableCommand = DEFAULT_COMMAND;
      this.jvmOptions = new ArrayList<>();
    }

    public Builder setJarPath(String jarPath) {
      this.jarPath = jarPath;
      return this;
    }

    public Builder setExecutableCommand(String executableCommand) {
      this.executableCommand = executableCommand;
      return this;
    }

    public Builder addJvmOptions(String... options) {
      jvmOptions.addAll(Arrays.asList(options));
      return this;
    }

    public JarBasedProcessJobletRunner build() {
      return new JarBasedProcessJobletRunner(executableCommand, jarPath, jvmOptions);
    }
  }

  public static Builder builder(String jarPath) {
    return new Builder(jarPath);
  }
}
