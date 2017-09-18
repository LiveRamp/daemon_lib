package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletConfig;

public class DefaultExecutionContext<T extends JobletConfig> implements ExecutionContext<T> {

  private T config;

  public DefaultExecutionContext(T config) {
    this.config = config;
  }

  @Override
  public T getConfig() {
    return config;
  }
}
