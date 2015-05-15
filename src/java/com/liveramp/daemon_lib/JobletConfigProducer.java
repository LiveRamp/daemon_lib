package com.liveramp.daemon_lib;

public interface JobletConfigProducer<T extends JobletConfig> {
  public T getNextConfig();
}
