package com.liveramp.daemon_lib;

public interface JobletFactory<T extends JobletConfig> {
  public abstract Joblet create(T config);
}
