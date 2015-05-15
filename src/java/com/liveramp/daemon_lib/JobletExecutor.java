package com.liveramp.daemon_lib;

public interface JobletExecutor<T extends JobletConfig> {
  public void execute(T config);

  public boolean canExecuteAnother();
}
