package com.liveramp.daemon_lib.config_serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.lang.SerializationUtils;

import com.liveramp.daemon_lib.JobletConfig;

public class JavaJobletConfigSerializer<T extends JobletConfig> implements JobletConfigSerializer<T> {
  @Override
  public byte[] serialize(T jobletConfig) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      SerializationUtils.serialize(jobletConfig, byteArrayOutputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  @Override
  public T deserialize(byte[] serializedConfig) throws IOException {
    try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(serializedConfig))) {
      return (T)objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
