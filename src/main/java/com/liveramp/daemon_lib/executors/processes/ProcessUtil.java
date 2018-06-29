package com.liveramp.daemon_lib.executors.processes;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.lang.NotImplementedException;

public class ProcessUtil {

  public static int run(ProcessBuilder processBuiler) throws IOException {
    Process process = startProcess(processBuiler);
    return extractPid(process);
  }

  public static int extractPid(Process process) throws IOException {
    int pid;
    // Non portable way to get process pid
    if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
      try {
        Field f = process.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        pid = f.getInt(process);
        return pid;
      } catch (Throwable e) {
        throw new IOException(e);
      }
    } else {
      throw new NotImplementedException("Don't support type: " + process.getClass().getName());
    }
  }

  public static Process startProcess(ProcessBuilder processBuiler) throws IOException {
    Process process = processBuiler.start();

    process.getInputStream().close();
    process.getErrorStream().close();
    process.getOutputStream().close();

    return process;
  }
}
