package com.liveramp.daemon_lib.executors.processes;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Consumer;

public interface IOConsumer<T> extends Consumer<T>, Serializable {

  @Override
  default void accept(T t) {
    try {
      this.acceptIO(t);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void acceptIO(T t) throws IOException;
}
