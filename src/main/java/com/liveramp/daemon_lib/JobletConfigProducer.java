package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

/*
JobletConfigProducers should be stateless.
For example, using the configBasedExecutionCondition would lead to unexpected behavior with a stateful configProducer.
*/
public interface JobletConfigProducer<T extends JobletConfig> {
  T getNextConfig() throws DaemonException;
}
