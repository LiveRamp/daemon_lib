package com.liveramp.daemon_lib.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Function;

import org.apache.commons.lang.SerializationUtils;

public class ThreadContextClassloaderDeserializer<T> implements Function<byte[], T> {
  @SuppressWarnings("unchecked")
  @Override
  public T apply(byte[] bytes) {
    try {
      return (T)SerializationUtils.deserialize(new ThreadContextClassloaderObjectInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
