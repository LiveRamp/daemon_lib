package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletCallback<T extends JobletConfig> {

  void callback(T config) throws DaemonException;

}
