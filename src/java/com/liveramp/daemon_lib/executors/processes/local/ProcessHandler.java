package com.liveramp.daemon_lib.executors.processes.local;

import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;

public interface ProcessHandler<T extends ProcessMetadata> {
  void onAdd(ProcessDefinition<T> watchedProcess);

  void onRemove(ProcessDefinition<T> watchedProcess);
}
