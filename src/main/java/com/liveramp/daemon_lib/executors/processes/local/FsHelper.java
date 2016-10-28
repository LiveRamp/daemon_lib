package com.liveramp.daemon_lib.executors.processes.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class FsHelper {
  private final String basePath;

  public FsHelper(String basePath) {
    this.basePath = basePath;
  }

  public File getBasePath() {
    return new File(basePath);
  }

  public <Pid> File getPidPath(Pid pid) {
    return new File(basePath, pid.toString());
  }

  public <Pid> File getPidTmpPath(Pid pid) {
    return new File(basePath, pid.toString() + "_tmp");
  }

  public void writeMetadata(File path, byte[] metadata) throws IOException {
    FileOutputStream output = new FileOutputStream(path);
    output.write(metadata);
    output.close();
  }

  public byte[] readMetadata(File path) throws IOException {
    FileInputStream input = new FileInputStream(path);
    byte[] bytes = new byte[1024];
    int read = input.read(bytes);
    if (read < 0) {
      throw new IOException("Could not read metadata from " + path);
    }
    input.close();
    return Arrays.copyOf(bytes, read);
  }
}
