package com.liveramp.daemon_lib.executors.processes;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface StatelessJobletExecutor<T extends JobletConfig> extends JobletExecutor<T> {

  default String initialize(T config) throws DaemonException {
    return null;
  }

  @Override
  default void execute(String initString, T config) throws DaemonException {
    execute(config);
  }

  void execute(T config) throws DaemonException;
}
