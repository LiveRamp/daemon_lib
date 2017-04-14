package com.liveramp.daemon_lib.executors.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

public class FileBasedReader implements Supplier<byte[]> {

  private final Path filePath;

  public FileBasedReader(Path filePath) {
    this.filePath = filePath;
  }

  @Override
  public byte[] get() {
    int tries = 0;

    Exception x = null;
    while (tries < 5) {
      tries++;
      try {
        return Files.readAllBytes(filePath);
      } catch (IOException e) {
        x = e;
        try {
          Thread.sleep(10);
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
      }
    }

    throw new RuntimeException(x);
  }

  @Override
  public String toString() {
    return String.format("{\"className\": \"%s\", \"path\": \"%s\"}", this.getClass().getSimpleName(), this.filePath);
  }
}
