package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletConfig;

public interface JobletExecutor<T extends JobletConfig> {
  public void execute(T config);

  public boolean canExecuteAnother();
}
