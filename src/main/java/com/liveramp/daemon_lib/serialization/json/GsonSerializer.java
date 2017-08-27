package com.liveramp.daemon_lib.serialization.json;

import java.nio.charset.Charset;
import java.util.function.Function;

import com.google.gson.Gson;

import com.liveramp.daemon_lib.JobletConfig;

public class GsonSerializer<T extends JobletConfig> implements Function<T, byte[]> {

  private final Class<T> type;
  private final Gson gson = new Gson();

  public GsonSerializer(Class<T> type) {
    this.type = type;
  }

  @Override
  public byte[] apply(T t) {
    return gson.toJson(new TypedConfigContainer<>(type.getName(), t)).getBytes(Charset.forName("UTF-8"));
  }
}
