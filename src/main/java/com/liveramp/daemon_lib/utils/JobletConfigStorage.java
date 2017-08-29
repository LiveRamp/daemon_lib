package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.serialization.JavaObjectDeserializer;
import com.liveramp.daemon_lib.serialization.JavaObjectSerializer;

public class JobletConfigStorage<T extends JobletConfig> {
  public static final Function<JobletConfig, byte[]> DEFAULT_SERIALIZER = new JavaObjectSerializer<>();

  private final String basePath;
  private final Function<? super T, byte[]> serializer;
  private final Function<byte[], ? super T> deserializer;

  public JobletConfigStorage(String basePath) {
    this(basePath, DEFAULT_SERIALIZER, getDefaultDeserializer());
  }

  public JobletConfigStorage(String basePath,
                             Function<? super T, byte[]> serializer,
                             Function<byte[], ? super T> deserializer) {
    this.basePath = basePath;
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  // Stores config and returns an identifier that can be used to retrieve it
  public String storeConfig(T config) throws IOException {
    String identifier = createIdentifier(config);
    try {
      File path = getPath(identifier);
      FileUtils.forceMkdir(path.getParentFile());
      FileUtils.writeByteArrayToFile(path, serializer.apply(config));
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }

    return identifier;
  }

  @SuppressWarnings("unchecked")
  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {
    try {

      final byte[] storedBytes = FileUtils.readFileToByteArray(getPath(identifier));
      return (T)deserializer.apply(storedBytes);

    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }
  }

  public void deleteConfig(String identifier) throws IOException {
    final File file = getPath(identifier);
    if (!file.delete()) {
      throw new IOException(String.format("Failed to delete configuration for id %s at %s", identifier, file.getPath()));
    }
  }

  public String getPath() {
    return basePath;
  }

  public static <T extends JobletConfig> JobletConfigStorage<T> production(String path) {
    return production(path, JobletConfigStorage.DEFAULT_SERIALIZER, getDefaultDeserializer());
  }

  public static <T extends JobletConfig> JobletConfigStorage<T> production(String path,
                                                                           Function<? super T, byte[]> serializer,
                                                                           Function<byte[], ? super T> deserializer) {
    return new JobletConfigStorage<>(path, serializer, deserializer);
  }

  private String createIdentifier(JobletConfig config) {
    return String.valueOf(Math.abs((long)config.hashCode())) + String.valueOf(System.nanoTime());
  }

  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }

  @SuppressWarnings("unchecked")
  @NotNull
  public static <T extends JobletConfig> Function<byte[], T> getDefaultDeserializer() {
    return new JavaObjectDeserializer<>();
  }

}
