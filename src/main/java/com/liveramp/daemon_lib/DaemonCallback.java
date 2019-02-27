package com.liveramp.daemon_lib;

import java.util.concurrent.Callable;

public interface DaemonCallback extends Callable<Void> {

  class None implements DaemonCallback {
    @Override
    public Void call() {
      return null;
    }
  }

}
