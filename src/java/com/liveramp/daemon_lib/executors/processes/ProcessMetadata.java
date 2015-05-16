package com.liveramp.daemon_lib.executors.processes;

public interface ProcessMetadata {
  public interface Serializer<T extends ProcessMetadata> {
    public byte[] toBytes(T metadata);

    public T fromBytes(byte[] bytes);
  }
}
