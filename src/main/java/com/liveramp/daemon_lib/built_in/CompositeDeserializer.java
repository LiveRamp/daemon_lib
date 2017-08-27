package com.liveramp.daemon_lib.built_in;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.JobletConfig;

public class CompositeDeserializer<T> implements Function<byte[], T> {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeDeserializer.class);

  private List<Function<byte[], ? extends T>> deserializersInOrder;

  public CompositeDeserializer(List<Function<byte[], ? extends T>> deserializersInOrder) {
    this.deserializersInOrder = deserializersInOrder;
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public CompositeDeserializer(Function<byte[], JobletConfig>... deserializersInOrder) {
    this.deserializersInOrder = Lists.newArrayList();
    this.deserializersInOrder.addAll(Arrays.stream(deserializersInOrder).map(c -> (Function<byte[], T>)c).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public T apply(byte[] bytes) {

    List<Throwable> exceptions = Lists.newArrayList();

    for (Function<byte[], ? extends T> deserializer : deserializersInOrder) {
      try {
        return deserializer.apply(bytes);
      } catch (Throwable t) {
        LOG.debug("Couldn't deserialize with " + deserializer, t);
        exceptions.add(t);
      }
    }

    throw new IllegalArgumentException("No deserializer worked. Failures were " + exceptions.stream().map(ExceptionUtils::getFullStackTrace).collect(Collectors.toList()));
  }

}
