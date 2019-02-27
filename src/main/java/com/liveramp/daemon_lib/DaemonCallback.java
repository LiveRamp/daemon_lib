package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface DaemonCallback {

  void callback() throws DaemonException;

  class None implements DaemonCallback {

    @Override
    public void callback() throws DaemonException {

    }
  }

}
