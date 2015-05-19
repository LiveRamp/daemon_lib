package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletFactory<T extends JobletConfig> {
  public abstract Joblet create(T config) throws DaemonException;
}
