package com.liveramp.daemon_lib.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SerializationUtils;

import com.liveramp.daemon_lib.JobletConfig;

public class JobletConfigStorage<T extends JobletConfig> {
  private final String basePath;

  public JobletConfigStorage(String basePath) {
    this.basePath = basePath;
  }

  // Stores config and returns an identifier that can be used to retrieve it
  public String storeConfig(T config) throws IOException {
    String identifier = createIdentifier(config);
    try {
      File path = getPath(identifier);
      FileUtils.forceMkdir(path.getParentFile());
      FileOutputStream fos = new FileOutputStream(path);
      SerializationUtils.serialize(config, fos);
      fos.close();
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }

    return identifier;
  }

  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {
    try {
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(getPath(identifier)));
      T config = (T)ois.readObject();
      ois.close();

      return config;
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return basePath;
  }

  public static <T extends JobletConfig> JobletConfigStorage<T> production(String path) {
    return new JobletConfigStorage<>(path);
  }

  private String createIdentifier(JobletConfig config) {
    return String.valueOf(config.hashCode()) + String.valueOf(System.nanoTime());
  }

  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }
}
