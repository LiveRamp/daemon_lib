package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletConfigProducer<T extends JobletConfig> {
  public T getNextConfig() throws DaemonException;
}
