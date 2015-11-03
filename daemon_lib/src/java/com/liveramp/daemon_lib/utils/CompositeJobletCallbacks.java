package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletCallbacks;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.utils.DaemonException;

public class CompositeJobletCallbacks<T extends JobletConfig> implements JobletCallbacks<T> {
  private final JobletCallback<T> before;
  private final JobletCallback<T> after;

  public CompositeJobletCallbacks(JobletCallback<T> before, JobletCallback<T> after) {
    this.before = before;
    this.after = after;
  }

  @Override
  public void before(T config) throws DaemonException {
    before.callback(config);
  }

  @Override
  public void after(T config) throws DaemonException {
    after.callback(config);
  }
}
