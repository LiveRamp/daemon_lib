package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;
import java.util.Map;

import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public interface ProcessJobletRunner {
  int run(Class<? extends JobletFactory<? extends JobletConfig>> jobletFactoryClass, JobletConfigStorage configStore, String cofigIdentifier, Map<String, String> envVariables, String workingDir) throws IOException, ClassNotFoundException;
}
