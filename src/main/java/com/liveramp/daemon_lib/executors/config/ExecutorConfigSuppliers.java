package com.liveramp.daemon_lib.executors.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.Lists;

public class ExecutorConfigSuppliers {
  public static final Path DEFAULT_SUB_PATH = Paths.get("default");

  public static <T extends ExecutorConfig> Supplier<T> standard(Path basePath, Class<T> klass) {
    return new ExecutorConfigSupplier<>(new GsonDeserializer<>(klass), new FileBasedReader(basePath.resolve(DEFAULT_SUB_PATH)));
  }

  public static <T extends ExecutorConfig> Supplier<T> hostBased(Path basePath, Class<T> klass) throws UnknownHostException {
    return fallingBack(
        standard(basePath.resolve(InetAddress.getLocalHost().getHostName()), klass),
        standard(basePath.resolve(DEFAULT_SUB_PATH), klass)
    );
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T extends ExecutorConfig> Supplier<T> fallingBack(Supplier<T> first, Supplier<T>... rest) {
    final List<Supplier<T>> suppliers = Lists.newArrayList(first);
    suppliers.addAll(Arrays.asList(rest));
    return new FallbackSupplier<>(suppliers);
  }
}
