package com.liveramp.daemon_lib.serialization;

import java.util.function.Function;

import org.apache.commons.lang3.SerializationUtils;

import com.liveramp.daemon_lib.JobletConfig;

public class JavaObjectDeserializer<T extends JobletConfig> implements Function<byte[], T> {
  @SuppressWarnings("unchecked")
  @Override
  public T apply(byte[] bytes) {
    return (T)SerializationUtils.deserialize(bytes);
  }
}
