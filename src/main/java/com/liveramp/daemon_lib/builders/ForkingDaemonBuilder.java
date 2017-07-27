package com.liveramp.daemon_lib.builders;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletConfigProducer;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.forking.ProcessJobletRunner;

public class ForkingDaemonBuilder<T extends JobletConfig> extends BaseForkingDaemonBuilder<T, ForkingDaemonBuilder<T>> {
  public ForkingDaemonBuilder(String workingDir, String identifier, Class<? extends JobletFactory<T>> jobletFactoryClass, JobletConfigProducer<T> configProducer, ProcessJobletRunner jobletRunner) {
    super(workingDir, identifier, jobletFactoryClass, configProducer, jobletRunner);
  }

  @Override
  protected ForkingDaemonBuilder<T> self() {
    return this;
  }
}
