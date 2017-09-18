package com.liveramp.daemon_lib.executors.processes;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.executors.ExecutionContext;
import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface StatelessJobletExecutor<T extends JobletConfig> extends JobletExecutor<T> {

  default ExecutionContext<T> createContext(T config) throws DaemonException {
    return new JustTheConfigContext<>(config);
  }

  @Override
  default void execute(ExecutionContext<T> context) throws DaemonException {
    execute(context.getConfig());
  }

  void execute(T config) throws DaemonException;

  class JustTheConfigContext<T extends JobletConfig> implements ExecutionContext<T> {

    private T config;

    public JustTheConfigContext(T config) {
      this.config = config;
    }

    @Override
    public T getConfig() {
      return config;
    }
  }
}
