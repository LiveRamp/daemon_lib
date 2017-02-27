package com.liveramp.daemon_lib.executors.config;

import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FallbackSupplier<T> implements Supplier<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FallbackSupplier.class);

  private final List<Supplier<T>> suppliers;

  public FallbackSupplier(List<Supplier<T>> suppliers) {
    this.suppliers = suppliers;
  }

  @Override
  public T get() {
    for (Supplier<T> supplier : suppliers) {
      try {
        final T t = supplier.get();
        if (t != null) {
          return t;
        } else {
          LOG.debug("Supplier {} doesn't have config", supplier);
        }
      } catch (Exception e) {
        LOG.warn("Supplier {} threw error {}", supplier, e);
      }
    }

    return null;
  }

  @Override
  public String toString() {
    return String.format("{\"className\": \"%s\", \"suppliers\": %s}", this.getClass().getSimpleName(), this.suppliers);
  }
}
