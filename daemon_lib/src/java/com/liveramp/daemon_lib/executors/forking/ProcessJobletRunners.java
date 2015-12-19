package com.liveramp.daemon_lib.executors.forking;

import com.liveramp.daemon_lib.utils.ForkedJobletRunner;

public class ProcessJobletRunners {
  public static ProcessJobletRunner production() {
    return new ForkedJobletRunner();
  }
}
