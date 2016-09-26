package com.liveramp.daemon_lib.executors.processes.local;

import java.util.Map;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.executors.processes.MetadataFactory;
import com.liveramp.daemon_lib.utils.JobletConfigMetadata;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class JobletConfigMetadataFactory implements MetadataFactory<JobletConfigMetadata> {
  @Override
  public JobletConfigMetadata createMetadata(String configStorageIdentifier, Class<? extends JobletFactory<? extends JobletConfig>> factoryClass, JobletConfigStorage storage, Map<String, String> envVariables) {
    return new JobletConfigMetadata(configStorageIdentifier);
  }
}
