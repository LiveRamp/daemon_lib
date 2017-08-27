package com.liveramp.daemon_lib.serialization;

import java.util.function.Function;

import org.apache.commons.lang.SerializationUtils;

import com.liveramp.daemon_lib.JobletConfig;

public class JavaObjectSerializer<T extends JobletConfig> implements Function<T, byte[]> {
  @Override
  public byte[] apply(T t) {
    return SerializationUtils.serialize(t);
  }
}
