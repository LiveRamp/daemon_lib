package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletConfig;

public class JobletCallbackUtil {

  @SafeVarargs
  public static <T extends JobletConfig> JobletCallback<T> compose(JobletCallback<T>... callbacks) {
    return new ComposeJobletCallback<>(callbacks);
  }

  static class ComposeJobletCallback<T extends JobletConfig> implements JobletCallback<T> {

    private final JobletCallback<T>[] callbacks;

    public ComposeJobletCallback(JobletCallback<T>... callbacks) {
      this.callbacks = callbacks;
    }

    @Override
    public void callback(T config) throws DaemonException {
      for (JobletCallback<T> callback : callbacks) {
        callback.callback(config);
      }
    }
  }

}
