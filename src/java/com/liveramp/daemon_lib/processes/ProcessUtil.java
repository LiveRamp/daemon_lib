package com.liveramp.daemon_lib.processes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtil {
  private static Logger LOG = LoggerFactory.getLogger(ProcessUtil.class);

  public static int runCommand(String... command) throws IOException {
    LOG.info("Run \"" + Arrays.toString(command) + "\" in " + System.getProperty("user.dir"));

    Process process = new ProcessBuilder(command).start();

    process.getInputStream().close();
    process.getErrorStream().close();
    process.getOutputStream().close();

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

  /**
   * Runs a command that is expected to echo the PID the consumer should be interested in. Assumes that the command
   * is non-blocking and echos the PID of some child process. This is useful when executing a shell script wrapper
   * but interested in the PID of the process it starts.
   *
   * @param command
   * @return
   * @throws java.io.IOException
   */
  public static int runPidEchoingCommand(String... command) throws IOException {
    LOG.info("Run \"" + Arrays.toString(command) + "\" in " + System.getProperty("user.dir"));

    Process process = new ProcessBuilder(command).start();

    try {
      process.waitFor();

      process.getOutputStream().close();
      process.getErrorStream().close();
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      int pid = Integer.parseInt(reader.readLine());
      reader.close();

      return pid;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
