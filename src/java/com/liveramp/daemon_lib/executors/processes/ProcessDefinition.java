package com.liveramp.daemon_lib.executors.processes;

public class ProcessDefinition<T extends ProcessMetadata, Pid> {
  private final Pid pid;
  private final T metadata;

  public ProcessDefinition(Pid pid, T metadata) {
    this.pid = pid;
    this.metadata = metadata;
  }

  public Pid getPid() {
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
