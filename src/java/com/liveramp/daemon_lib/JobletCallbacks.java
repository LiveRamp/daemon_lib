package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

@Deprecated
public interface JobletCallbacks<T extends JobletConfig> {
  void before(T config) throws DaemonException;

  void after(T config) throws DaemonException;

  class None<T extends JobletConfig> implements JobletCallbacks<T> {

    @Override
    public void before(T config) throws DaemonException {
      // do nothing
    }

    @Override
    public void after(T config) throws DaemonException {
      // do nothing
    }
  }
}
