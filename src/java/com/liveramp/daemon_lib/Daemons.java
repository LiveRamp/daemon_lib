package com.liveramp.daemon_lib;

import java.io.File;
import java.io.IOException;

import com.liveramp.daemon_lib.executors.JobletExecutors;

public class Daemons {
  public static class Forked<T extends JobletConfig> {
    private String workingDir;
    private String identifier;
    private int maxProcesses;
    private Class<? extends JobletFactory<T>> jobletFactoryClass;
    private JobletConfigProducer<T> jobletConfigProducer;

    public Forked(String workingDir, String identifier, int maxProcesses, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer) {
      this.workingDir = workingDir;
      this.identifier = identifier;
      this.maxProcesses = maxProcesses;
      this.jobletFactoryClass = jobletFactoryClass;
      this.jobletConfigProducer = jobletConfigProducer;
    }

    public Daemon get() throws IllegalAccessException, IOException, InstantiationException {
      final String tmpPath = new File(workingDir, identifier).getPath();
      return new Daemon<T>(
          identifier,
          JobletExecutors.Forked.get(tmpPath, maxProcesses, jobletFactoryClass),
          jobletConfigProducer
      );
    }
  }

  public static <T extends JobletConfig> Daemon forked(String workingDir, String identifier, int maxProcess, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> jobletConfigProducer) throws IOException, InstantiationException, IllegalAccessException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return new Daemon<T>(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactoryClass),
        jobletConfigProducer
    );
  }
}
