package com.liveramp.daemon_lib.executors.forking;

public class ProcessJobletRunners {
  public static ProcessJobletRunner production() {
    return new ForkedJobletRunner();
  }

  public static ProcessJobletRunner experimental() {
    return new DefaultProcessJobletRunner();
  }
}
