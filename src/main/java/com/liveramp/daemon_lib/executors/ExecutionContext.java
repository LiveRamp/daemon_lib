package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletConfig;

public interface ExecutionContext<T extends JobletConfig> {

  T getConfig();

}
