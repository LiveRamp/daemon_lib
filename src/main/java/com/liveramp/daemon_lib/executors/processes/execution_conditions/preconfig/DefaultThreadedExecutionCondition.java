package com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig;

import java.util.concurrent.ThreadPoolExecutor;

import com.liveramp.daemon_lib.executors.ThreadedJobletExecutor;

public class DefaultThreadedExecutionCondition implements ExecutionCondition {
  private final ThreadPoolExecutor threadPool;
  private final ThreadedJobletExecutor.Config threadedExecutorConfig;

  public DefaultThreadedExecutionCondition(ThreadPoolExecutor threadPool, ThreadedJobletExecutor.Config threadedExecutorConfig) {
    this.threadPool = threadPool;
    this.threadedExecutorConfig = threadedExecutorConfig;
  }

  @Override
  public boolean canExecute() {
    return threadPool.getActiveCount() < threadedExecutorConfig.numJoblets;
  }
}
