package com.liveramp.daemon_lib.config_serialization;

import java.io.IOException;

import com.google.gson.Gson;

import com.liveramp.daemon_lib.JobletConfig;

public class GsonJobletConfigSerializer<T extends JobletConfig> implements JobletConfigSerializer<T> {
  private final Class<T> clazz;

  public GsonJobletConfigSerializer(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public byte[] serialize(T jobletConfig) throws IOException {
    return new Gson().toJson(jobletConfig).getBytes();
  }

  @Override
  public T deserialize(byte[] serializedConfig) throws IOException {
    return new Gson().fromJson(new String(serializedConfig), clazz);
  }
}
