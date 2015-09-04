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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProcessDefinition<?> that = (ProcessDefinition<?>)o;

    if (pid != that.pid) {
      return false;
    }
    return metadata.equals(that.metadata);

  }

  @Override
  public int hashCode() {
    int result = pid;
    result = 31 * result + metadata.hashCode();
    return result;
  }
}
