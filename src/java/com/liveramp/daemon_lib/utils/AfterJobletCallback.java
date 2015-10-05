package com.liveramp.daemon_lib.utils;

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;

public class AfterJobletCallback<T extends JobletConfig> implements JobletCallback<T> {

  private final JobletCallbacks<T> callbacks;

  public AfterJobletCallback(JobletCallbacks<T> callbacks) {
    this.callbacks = callbacks;
  }

  @Override
  public void callback(T config) throws DaemonException {
    callbacks.after(config);
  }

  public static <T extends JobletConfig> JobletCallback<T> wrap(JobletCallbacks<T> callbacks) {
    return new AfterJobletCallback<T>(callbacks);
  }
}
