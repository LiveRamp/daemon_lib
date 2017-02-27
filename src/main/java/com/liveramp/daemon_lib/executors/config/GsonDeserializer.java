package com.liveramp.daemon_lib.executors.config;

import java.nio.charset.Charset;
import java.util.function.Function;

import com.google.gson.Gson;

public class GsonDeserializer<T> implements Function<byte[], T> {

  private final Class<T> klass;
  private final Gson gson;

  public GsonDeserializer(Class<T> klass) {
    this.klass = klass;
    this.gson = new Gson();
  }



  @Override
  public T apply(byte[] bytes) {
    return gson.fromJson(new String(bytes, Charset.forName("UTF-8")), klass);
  }

  @Override
  public String toString() {
    return String.format("{\"className\": \"%s\", \"classBeingDeserializedTo\": \"%s\"}", this.getClass().getSimpleName(), klass.getCanonicalName());
  }
}
