package com.liveramp.daemon_lib.builders;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;

public class BlockingDaemonBuilder<T extends JobletConfig> extends BaseBlockingDaemonBuilder<T, BlockingDaemonBuilder<T>> {
  public BlockingDaemonBuilder(String identifier, JobletFactory<T> jobletFactory, JobletConfigProducer<T> configProducer) {
    super(identifier, jobletFactory, configProducer);
  }

  @Override
  protected BlockingDaemonBuilder<T> self() {
    return this;
  }
}
