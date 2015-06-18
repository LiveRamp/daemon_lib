package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletCallbacks<T extends JobletConfig> {
  void before(T config) throws DaemonException;

  void after(T config) throws DaemonException;
}
