package com.liveramp.daemon_lib.executors.processes.execution_conditions.postconfig;

import com.google.common.base.Predicate;
import com.liveramp.daemon_lib.JobletConfig;

public interface ConfigBasedExecutionCondition<T extends JobletConfig> extends Predicate<T> {
}
