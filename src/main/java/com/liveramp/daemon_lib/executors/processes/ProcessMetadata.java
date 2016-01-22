package com.liveramp.daemon_lib.executors.processes;

public interface ProcessMetadata {
  interface Serializer<T extends ProcessMetadata> {
    byte[] toBytes(T metadata);

    T fromBytes(byte[] bytes);
  }
}
