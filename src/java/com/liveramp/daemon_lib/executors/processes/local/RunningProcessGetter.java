package com.liveramp.daemon_lib.executors.processes.local;

import java.util.Map;

public interface RunningProcessGetter<Pid, PidData> {

  class PidData {
    String command;

    @Override
    public String toString() {
      return "PidData{" +
          "command='" + command + '\'' +
          '}';
    }
  }

  Map<Pid, PidData> getPids() throws Exception;

}
