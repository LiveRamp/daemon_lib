package com.liveramp.daemon_lib.executors.processes;

import java.util.Map;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public interface MetadataFactory<M extends ProcessMetadata> {

  M createMetadata(String configStorageIdentifier, Class<? extends JobletFactory<? extends JobletConfig>> factoryClass, JobletConfigStorage storage, Map<String, String> envVariables);

}
