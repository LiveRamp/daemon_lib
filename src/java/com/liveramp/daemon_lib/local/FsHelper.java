package com.liveramp.daemon_lib.local;

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

  public File getPidPath(int pid) {
    return new File(basePath, Integer.valueOf(pid).toString());
  }

  public File getPidTmpPath(int pid) {
    return new File(basePath, Integer.valueOf(pid).toString() + "_tmp");
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
    input.close();
    return Arrays.copyOf(bytes, read);
  }
}
