package com.liveramp.daemon_lib;

import org.slf4j.Logger;

import com.liveramp.daemon_lib.utils.DaemonException;

import static org.slf4j.LoggerFactory.getLogger;

public interface JobletCallback<T extends JobletConfig> {

  void callback(T config) throws DaemonException;

  class None<T extends JobletConfig> implements JobletCallback<T> {

    @Override
    public void callback(T config) throws DaemonException {

    }
  }

  class Dummy<T extends JobletConfig> implements JobletCallback<T> {
    private static final Logger LOG = getLogger(Dummy.class);

    private final String message;

    public Dummy(String message) {
      this.message = message;
    }

    @Override
    public void callback(T config) throws DaemonException {
      LOG.info("Dummy Callback for config {}: {}", config.toString(), message);
    }
  }
}
