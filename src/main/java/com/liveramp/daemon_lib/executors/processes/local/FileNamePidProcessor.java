package com.liveramp.daemon_lib.executors.processes.local;

import com.google.common.base.Optional;

public interface FileNamePidProcessor<Pid> {

  Optional<Pid> processFileName(String filename);

}
