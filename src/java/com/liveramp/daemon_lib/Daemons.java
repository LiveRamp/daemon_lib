package com.liveramp.daemon_lib;

import java.io.File;
import java.io.IOException;

import com.liveramp.daemon_lib.executors.JobletExecutors;

public class Daemons {
  public static <T extends JobletConfig> Daemon forked(String workingDir, String identifier, int maxProcess, JobletFactory<T> jobletFactory, JobletConfigProducer<T> jobletConfigProducer) throws IOException {
    final String tmpPath = new File(workingDir, identifier).getPath();
    return new Daemon<T>(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactory),
        jobletConfigProducer
    );
  }
}
