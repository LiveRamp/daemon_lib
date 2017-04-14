package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.config_serialization.JavaJobletConfigSerializer;
import com.liveramp.daemon_lib.config_serialization.JobletConfigSerializer;

public class JobletConfigStorage<T extends JobletConfig> {
  private final String basePath;
  private final JobletConfigSerializer<T> jobletConfigSerializer;

  public JobletConfigStorage(String basePath, JobletConfigSerializer<T> jobletConfigSerializer) {
    this.basePath = basePath;
    this.jobletConfigSerializer = jobletConfigSerializer;
  }

  // Stores config and returns an identifier that can be used to retrieve it
  public String storeConfig(T config) throws IOException {
    String identifier = createIdentifier(config);
    try {
      File path = getPath(identifier);
      FileUtils.forceMkdir(path.getParentFile());
      try (OutputStream outputStream = new FileOutputStream(path)) {
        byte[] serializedConfig = jobletConfigSerializer.serialize(config);
        outputStream.write(serializedConfig);
      }
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }

    return identifier;
  }

  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {
    try {
      try (InputStream inputStream = new FileInputStream(getPath(identifier))) {
        byte[] serializedConfig = IOUtils.toByteArray(inputStream);
        return jobletConfigSerializer.deserialize(serializedConfig);
      }
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
    return production(path, new JavaJobletConfigSerializer<>());
  }

  public static <T extends JobletConfig> JobletConfigStorage<T> production(String path, JobletConfigSerializer<T> jobletConfigSerializer) {
    return new JobletConfigStorage<>(path, jobletConfigSerializer);
  }

  private String createIdentifier(JobletConfig config) {
    return String.valueOf(config.hashCode()) + String.valueOf(System.nanoTime());
  }

  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }
}

