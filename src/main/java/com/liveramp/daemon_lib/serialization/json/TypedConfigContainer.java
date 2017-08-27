package com.liveramp.daemon_lib.serialization.json;

import com.liveramp.daemon_lib.JobletConfig;

class TypedConfigContainer<T extends JobletConfig> {

  String className;
  Object config;

  TypedConfigContainer(String className, T config) {
    this.className = className;
    this.config = config;
  }

}
