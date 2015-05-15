package com.liveramp.daemon_lib;

import java.io.IOException;

import com.liveramp.daemon_lib.joblet_executors.JobletExecutors;

public class Daemons {
  private static final String BASE_TMP_PATH = "/tmp/daemons/";

  public static <T extends JobletConfig> Daemon forked(String identifier, int maxProcess, JobletFactory<T> jobletFactory, JobletConfigProducer<T> jobletConfigProducer) throws IOException {
    final String tmpPath = BASE_TMP_PATH + identifier;
    return new Daemon(
        identifier,
        JobletExecutors.Forked.get(tmpPath, maxProcess, jobletFactory),
        jobletConfigProducer
    );
  }
}
