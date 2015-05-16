package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletExecutor<T extends JobletConfig> {
  public void execute(T config) throws DaemonException;

  public boolean canExecuteAnother();
}
