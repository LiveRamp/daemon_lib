package com.liveramp.daemon_lib.executors.processes;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    return startProcess(processBuiler,
        new CloseStream<>(),
        new CloseStream<>(),
        new CloseStream<>());
  }

  public static Process startProcess(ProcessBuilder processBuiler,
                                     IOConsumer<InputStream> standardOutCallback,
                                     IOConsumer<InputStream> standardErrorCallback,
                                     IOConsumer<OutputStream> standardInputCallback) throws IOException {
    Process process = processBuiler.start();

    standardOutCallback.acceptIO(process.getInputStream());
    standardErrorCallback.acceptIO(process.getErrorStream());
    standardInputCallback.acceptIO(process.getOutputStream());

    return process;
  }

  private static class CloseStream<T extends Closeable> implements IOConsumer<T> {

    @Override
    public void acceptIO(T t) throws IOException {
      t.close();
    }
  }

}
