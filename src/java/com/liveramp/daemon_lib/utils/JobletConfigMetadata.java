package com.liveramp.daemon_lib.utils;

import com.google.gson.Gson;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;

public class JobletConfigMetadata implements ProcessMetadata {
  private final String identifier;

  public JobletConfigMetadata(String identifier) {
    this.identifier = identifier;
  }

  public String getIdentifier() {
    return identifier;
  }

  public static class Serializer implements ProcessMetadata.Serializer<JobletConfigMetadata> {
    private final Gson gson;

    public Serializer() {
      this.gson = new Gson();
    }

    @Override
    public byte[] toBytes(JobletConfigMetadata metadata) {
      return BytesUtils.stringToBytes(new Gson().toJson(metadata));
    }

    @Override
    public JobletConfigMetadata fromBytes(byte[] bytes) {
      return gson.fromJson(BytesUtils.bytesToString(bytes), JobletConfigMetadata.class);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "{ \"%s\": { \"identifier\": \"%s\"}}",
        JobletConfigMetadata.class.getName(),
        identifier
    );
  }
}
