package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;

import com.liveramp.daemon_lib.JobletConfig;

public class DiskJobletConfigStorage<T extends JobletConfig> extends BaseJobletConfigStorage<T> {
  private final String basePath;
  private final Function<? super T, byte[]> serializer;
  private final Function<byte[], ? super T> deserializer;

  public DiskJobletConfigStorage(String basePath) {
    this(basePath, DEFAULT_SERIALIZER, getDefaultDeserializer());
  }

  public DiskJobletConfigStorage(String basePath,
                             Function<? super T, byte[]> serializer,
                             Function<byte[], ? super T> deserializer) {
    this.basePath = basePath;
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public String storeConfig(T config) throws IOException {
    String identifier = createIdentifier(config);
    storeConfig(identifier, config);
    return identifier;
  }

  @Override
  public void storeConfig(String identifier, T config) throws IOException {
    try {
      File path = getPath(identifier);
      FileUtils.forceMkdir(path.getParentFile());
      FileUtils.writeByteArrayToFile(path, serializer.apply(config));
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<String> getStoredIdentifiers() throws IOException {
    File baseDir = new File(basePath);
    String[] keys = baseDir.list();
    if (keys == null) {
      if (baseDir.exists()) {
        throw new IOException("Error getting list of stored identifiers");
      } else {
        return Collections.emptySet();
      }
    }
    return Sets.newHashSet(keys);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {
    try {
      final byte[] storedBytes = FileUtils.readFileToByteArray(getPath(identifier));
      return (T)deserializer.apply(storedBytes);
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteConfig(String identifier) throws IOException {
    final File file = getPath(identifier);
    if (!file.delete()) {
      throw new IOException(String.format("Failed to delete configuration for id %s at %s", identifier, file.getPath()));
    }
  }

  @Override
  public String getPath() {
    return basePath;
  }

  public static <T extends JobletConfig> DiskJobletConfigStorage<T> production(String path) {
    return production(path, BaseJobletConfigStorage.DEFAULT_SERIALIZER, getDefaultDeserializer());
  }

  public static <T extends JobletConfig> DiskJobletConfigStorage<T> production(String path,
                                                                           Function<? super T, byte[]> serializer,
                                                                           Function<byte[], ? super T> deserializer) {
    return new DiskJobletConfigStorage<>(path, serializer, deserializer);
  }

  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }
}
