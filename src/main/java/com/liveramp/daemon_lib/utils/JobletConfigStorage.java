package com.liveramp.daemon_lib.utils;

import java.io.IOException;
import java.util.Set;

import com.liveramp.daemon_lib.JobletConfig;

public interface JobletConfigStorage<T extends JobletConfig> {
  // Stores config and returns an identifier that can be used to retrieve it
  String storeConfig(T config) throws IOException;

  void storeConfig(String identifier, T config) throws IOException;

  T loadConfig(String identifier) throws IOException, ClassNotFoundException;

  void deleteConfig(String identifier) throws IOException;

  String getPath();

  Set<String> getStoredIdentifiers() throws IOException;
}
