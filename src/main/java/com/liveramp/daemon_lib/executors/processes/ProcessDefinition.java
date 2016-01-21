package com.liveramp.daemon_lib.executors.processes;

public class ProcessDefinition<T extends ProcessMetadata> {
  private final int pid;
  private final T metadata;

  public ProcessDefinition(int pid, T metadata) {
    this.pid = pid;
    this.metadata = metadata;
  }

  public int getPid() {
    return pid;
  }

  public T getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "Process{" +
        "pid=" + getPid() +
        ", metadata=" + getMetadata() +
        '}';
  }
}
