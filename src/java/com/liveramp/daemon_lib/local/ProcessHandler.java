package com.liveramp.daemon_lib.local;

import com.liveramp.daemon_lib.processes.ProcessDefinition;
import com.liveramp.daemon_lib.processes.ProcessMetadata;

public interface ProcessHandler<T extends ProcessMetadata> {
  void onAdd(ProcessDefinition<T> watchedProcess);

  void onRemove(ProcessDefinition<T> watchedProcess);
}
