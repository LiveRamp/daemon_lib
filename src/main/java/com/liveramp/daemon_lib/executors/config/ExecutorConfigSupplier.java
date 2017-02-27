package com.liveramp.daemon_lib.executors.config;

import java.util.function.Function;
import java.util.function.Supplier;

public class ExecutorConfigSupplier<T extends ExecutorConfig> implements Supplier<T> {
  private final Function<byte[], T> deserializer;
  private final Supplier<byte[]> reader;

  public ExecutorConfigSupplier(Function<byte[], T> deserializer, Supplier<byte[]> reader) {
    this.deserializer = deserializer;
    this.reader = reader;
  }

  @Override
  public T get() {
    final byte[] bytes = reader.get();
    return deserializer.apply(bytes);
  }

  @Override
  public String toString() {
    return String.format("\"className\": \"%s\", \"deserializer\": %s, \"reader\": \"%s\"", this.getClass().getSimpleName(), deserializer, reader);
  }
}
