package com.liveramp.daemon_lib.serialization.json;

import java.nio.charset.Charset;
import java.util.function.Function;

import com.google.gson.Gson;

public class GsonDeserializer<T> implements Function<byte[], T> {
  private final Gson gson = new Gson();

  @SuppressWarnings("unchecked")
  @Override
  public T apply(byte[] bytes) {
    final String json = new String(bytes, Charset.forName("UTF-8"));
    // pull out the encapsulated form so we can get the classname
    final TypedConfigContainer tcc = gson.fromJson(json, TypedConfigContainer.class);
    try {
      final Class<T> klass = (Class<T>)Class.forName(tcc.className);
      // telling Gson which class it's deserializing is important to prevent it from making everything into Maps
      final String json1 = gson.toJson(tcc.config);
      return gson.fromJson(json1, klass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
