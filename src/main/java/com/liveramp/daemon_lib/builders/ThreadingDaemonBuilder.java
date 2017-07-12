package com.liveramp.daemon_lib.builders;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;

public class ThreadingDaemonBuilder<T extends JobletConfig> extends BaseThreadingDaemonBuilder<T, ThreadingDaemonBuilder<T>> {
  public ThreadingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer) {
    super(identifier, jobletFactory, configProducer);
  }

  @Override
  protected ThreadingDaemonBuilder<T> self() {
    return this;
  }
}
