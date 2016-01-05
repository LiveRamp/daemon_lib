package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import org.slf4j.Logger;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.ProcessUtil;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;
import com.liveramp.java_support.util.JarUtils;

import static org.slf4j.LoggerFactory.getLogger;

public class DefaultProcessJobletRunner implements ProcessJobletRunner {
  private static final Logger LOG = getLogger(DefaultProcessJobletRunner.class);

  @Override
  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier, Map<String, String> envVariables, String workingDir) throws IOException {
    String separator = System.getProperty("file.separator");
    String classpath = getClasspath();
    String path = System.getProperty("java.home")
        + separator + "bin" + separator + "java";
    ProcessBuilder processBuilder =
        new ProcessBuilder(path, "-cp",
            classpath,
            ForkedJobletRunner.class.getName(),
            jobletFactoryClass.getName(),
            configStore.getPath(),
            workingDir,
            cofigIdentifier);

    processBuilder.environment().putAll(envVariables);

    LOG.info("Running command: {}", processBuilder.command());

    int pid = ProcessUtil.run(processBuilder);

    return pid;
  }

  private String getClasspath() throws ClassNotFoundException {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    List<URL> urls = Arrays.asList(((URLClassLoader)cl).getURLs());
    urls.add(JarUtils.getMainJarURL());

    List<String> paths = new ArrayList<>();
    for (URL url : urls) {
      paths.add(url.getPath());
    }

    return Joiner.on(":").join(paths);
  }

  public static void main(String[] args) throws ClassNotFoundException {
    System.out.println(new DefaultProcessJobletRunner().getClasspath());
  }

}
