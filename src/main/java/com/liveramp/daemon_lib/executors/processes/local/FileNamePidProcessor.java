package com.liveramp.daemon_lib.executors.processes.local;

import java.util.Optional;

public interface FileNamePidProcessor<Pid> {

  Optional<Pid> processFileName(String filename);

}
