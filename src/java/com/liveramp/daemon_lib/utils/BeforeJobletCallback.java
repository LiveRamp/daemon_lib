package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;

public class BeforeJobletCallback<T extends JobletConfig> implements JobletCallback<T> {

  private final JobletCallbacks<T> callbacks;

  public BeforeJobletCallback(JobletCallbacks<T> callbacks) {
    this.callbacks = callbacks;
  }

  @Override
  public void callback(T config) throws DaemonException {
    callbacks.before(config);
  }

  public static <T extends JobletConfig> JobletCallback<T> wrap(JobletCallbacks<T> callbacks) {
    return new BeforeJobletCallback<T>(callbacks);
  }
}
