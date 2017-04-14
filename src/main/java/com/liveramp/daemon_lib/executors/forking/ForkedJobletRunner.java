package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.config_serialization.JavaJobletConfigSerializer;
import com.liveramp.daemon_lib.tracking.DefaultJobletStatusManager;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.JobletConfigStorage;

public class ForkedJobletRunner {
  public static String quote(String s) {
    return "'" + s + "'";
  }

  private static String unquote(String s) {
    if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
      return s.substring(1, s.length() - 1);
    } else {
      return s;
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, DaemonException {

    String jobletFactoryClassName = unquote(args[0]);
    String configStorePath = args[1];
    String daemonWorkingDir = args[2];
    String id = args[3];

    JobletFactory factory = (JobletFactory)Class.forName(jobletFactoryClassName).newInstance();
    JobletConfig config = JobletConfigStorage.production(configStorePath, new JavaJobletConfigSerializer<>()).loadConfig(id);
    DefaultJobletStatusManager jobletStatusManager = new DefaultJobletStatusManager(daemonWorkingDir);

    jobletStatusManager.start(id);
    Joblet joblet = factory.create(config);
    joblet.run();
    jobletStatusManager.complete(id);
  }
}
