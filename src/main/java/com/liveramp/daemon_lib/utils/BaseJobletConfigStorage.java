package com.liveramp.daemon_lib.utils;

import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.built_in.CompositeDeserializer;
import com.liveramp.daemon_lib.serialization.JavaObjectDeserializer;
import com.liveramp.daemon_lib.serialization.JavaObjectSerializer;
import com.liveramp.daemon_lib.serialization.ThreadContextClassloaderDeserializer;

public abstract class BaseJobletConfigStorage<T extends JobletConfig> implements JobletConfigStorage<T> {
  public static final Function<JobletConfig, byte[]> DEFAULT_SERIALIZER = new JavaObjectSerializer<>();

  protected String createIdentifier(JobletConfig config) {
    return String.valueOf(Math.abs((long)config.hashCode())) + String.valueOf(System.nanoTime());
  }

  @SuppressWarnings("unchecked")
  @NotNull
  public static <T extends JobletConfig> Function<byte[], T> getDefaultDeserializer() {
    return new CompositeDeserializer<>(new JavaObjectDeserializer<>(), new ThreadContextClassloaderDeserializer<>());
  }
}
