package com.liveramp.daemon_lib.executors.processes;

import java.util.List;

public interface ProcessController<T extends ProcessMetadata, Pid> {
  void registerProcess(Pid pid, T metadata) throws ProcessControllerException;

  List<ProcessDefinition<T, Pid>> getProcesses() throws ProcessControllerException;
}
