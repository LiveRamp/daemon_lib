package com.liveramp.daemon_lib.built_in;

import java.util.List;

import com.google.common.base.Joiner;

import com.liveramp.daemon_lib.JobletConfig;

public class IDsConfig implements JobletConfig {
  private final List<Long> ids;

  public IDsConfig(List<Long> ids) {
    this.ids = ids;
  }

  public List<Long> getIds() {
    return ids;
  }

  @Override
  public String toString() {
    return "IDsConfig{" +
        "ids=" + Joiner.on(",").join(ids) +
        '}';
  }
}
