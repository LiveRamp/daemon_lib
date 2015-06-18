package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletExecutor<T extends JobletConfig> {
  void execute(T config) throws DaemonException;

  boolean canExecuteAnother();
}
