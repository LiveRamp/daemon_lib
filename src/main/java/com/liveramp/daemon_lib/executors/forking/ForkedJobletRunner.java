package com.liveramp.daemon_lib.executors.forking;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletConfig;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.built_in.CompositeDeserializer;
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
    String customSerializationClasses = args.length > 4 ? args[4] : "";

    DefaultJobletStatusManager jobletStatusManager = new DefaultJobletStatusManager(daemonWorkingDir);
    jobletStatusManager.start(id);

    JobletFactory factory = (JobletFactory)Class.forName(jobletFactoryClassName).newInstance();
    final List<Function<byte[], ? extends JobletConfig>> deserializersWithDefault = Stream.concat(
        Stream.of(customSerializationClasses.split(";")).map(ForkedJobletRunner::getInstanceOfDeserializer),
        Stream.of(JobletConfigStorage.getDefaultDeserializer()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    JobletConfig config = JobletConfigStorage.production(configStorePath,
        null,
        new CompositeDeserializer<>(deserializersWithDefault))
        .loadConfig(id);

    Joblet joblet = factory.create(config);
    joblet.run();
    jobletStatusManager.complete(id);
  }

  @SuppressWarnings("unchecked")
  private static Function<byte[], JobletConfig> getInstanceOfDeserializer(String s) {
    try {
      return (Function<byte[], JobletConfig>)Class.forName(s).newInstance();
    } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
      System.err.println("Could not find class " + s);
      System.err.println("Ignoring deserializer " + s);
      return null;
    }
  }

}
