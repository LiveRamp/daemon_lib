package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletCallbacks<T extends JobletConfig> {
  public void before(T config) throws DaemonException;

  public void after(T config) throws DaemonException;
}
