package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;

public class JobletCallbacks {

  public <T extends JobletConfig> JobletCallback<T> compose(JobletCallback<T> first, JobletCallback<T> second) {
    return new ComposeJobletCallback<>(first, second);
  }

  class ComposeJobletCallback<T extends JobletConfig> implements JobletCallback<T> {

    private final JobletCallback<T> first;
    private final JobletCallback<T> second;

    public ComposeJobletCallback(JobletCallback<T> first, JobletCallback<T> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public void callback(T config) throws DaemonException {
      first.callback(config);
      second.callback(config);
    }
  }

}
