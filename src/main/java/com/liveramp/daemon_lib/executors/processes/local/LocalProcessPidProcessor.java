package com.liveramp.daemon_lib.executors.processes.local;

import java.util.Optional;

public class LocalProcessPidProcessor implements FileNamePidProcessor<Integer> {
  @Override
  public Optional<Integer> processFileName(String filename) {
    if (filename.matches("\\d+")) {
      return Optional.of(Integer.parseInt(filename));
    } else {
      return Optional.empty();
    }
  }
}
