package com.liveramp.daemon_lib.executors.forking;

public class ProcessJobletRunners {
  public static ProcessJobletRunner production() {
    return new ForkedJobletRunner();
  }
}
