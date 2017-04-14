package com.liveramp.daemon_lib.config_serialization;

import java.io.IOException;

import com.liveramp.daemon_lib.JobletConfig;

public interface JobletConfigSerializer<T extends JobletConfig> {
  /**
   * Writes a serialized version of the config to the output stream, then closes the output stream.
   *
   * @param jobletConfig
   */
  byte[] serialize(T jobletConfig) throws IOException;

  /**
   * Reads a serialized config from the input stream, closes the stream, and returns the config.
   *
   * @param serializedConfig
   * @return
   */
  T deserialize(byte[] serializedConfig) throws IOException;
}
