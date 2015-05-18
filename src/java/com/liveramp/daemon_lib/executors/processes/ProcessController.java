package com.liveramp.daemon_lib.executors.processes;

import java.util.List;

public interface ProcessController<T extends ProcessMetadata> {
  public void registerProcess(int pid, T metadata) throws ProcessControllerException;

  List<ProcessDefinition<T>> getProcesses();
}
