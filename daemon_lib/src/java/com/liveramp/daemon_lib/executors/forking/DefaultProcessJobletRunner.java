package com.liveramp.daemon_lib.executors.forking;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
  public int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier, Map<String, String> envVariables, String workingDir) throws IOException, ClassNotFoundException {
    String separator = System.getProperty("file.separator");
    String classpath = getClasspath();
    String path = System.getProperty("java.home")
        + separator + "bin" + separator + "java";
    ProcessBuilder processBuilder =
        new ProcessBuilder(path, "-cp",
            classpath,
            ForkedJobletRunner.class.getName(),
            quote(jobletFactoryClass.getName()),
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

  private static String quote(String s) {
    return "'" + s + "'";
  }

  private String getClasspath() throws ClassNotFoundException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();

    List<URL> urls = Lists.newArrayList(((URLClassLoader)cl).getURLs());

    List<String> paths = new ArrayList<>();
    paths.add(JarUtils.getMainJarURL().getPath());
    for (URL url : urls) {
      paths.add(url.getPath());
    }

    return Joiner.on(":").join(paths);
  }

  public static void main(String[] args) throws ClassNotFoundException {
    System.out.println(new DefaultProcessJobletRunner().getClasspath());
  }

}
