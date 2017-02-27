package com.liveramp.daemon_lib.executors.config;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.Lists;

public class ExecutorConfigSuppliers {
  public static <T extends ExecutorConfig> Supplier<T> standard(Path filename, Class<T> klass) {
    return new ExecutorConfigSupplier<>(new GsonDeserializer<>(klass), new FileBasedReader(filename));
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T extends ExecutorConfig> Supplier<T> fallingBack(Supplier<T> first, Supplier<T>... rest) {
    final List<Supplier<T>> suppliers = Lists.newArrayList(first);
    suppliers.addAll(Arrays.asList(rest));
    return new FallbackSupplier<>(suppliers);
  }
}
