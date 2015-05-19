package com.liveramp.daemon_lib.built_in;

import com.liveramp.daemon_lib.JobletConfig;

public class IDConfig implements JobletConfig {
  private final long id;

  public IDConfig(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return "IDConfig{" +
        "id=" + id +
        '}';
  }
}
