package com.liveramp.daemon_lib;

public abstract class JobletFactory<T extends JobletConfig> {
  public JobletFactory() {
  }

  public abstract Joblet create(T config);
}
