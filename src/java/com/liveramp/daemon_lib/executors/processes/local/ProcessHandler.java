package com.liveramp.daemon_lib.executors.processes.local;

import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;
import com.liveramp.daemon_lib.utils.DaemonException;

public interface ProcessHandler<T extends ProcessMetadata, Pid> {
  void onRemove(ProcessDefinition<T, Pid> watchedProcess) throws DaemonException;
}
