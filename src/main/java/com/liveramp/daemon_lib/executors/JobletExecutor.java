package com.liveramp.daemon_lib.executors;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionCondition;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface JobletExecutor<T extends JobletConfig> {

  ExecutionContext<T> createContext(T config) throws DaemonException;

  void execute(ExecutionContext<T> context) throws DaemonException;

  ExecutionCondition getDefaultExecutionCondition();

  void shutdown();
}
