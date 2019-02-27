package com.liveramp.daemon_lib;

public interface DaemonCallback extends Runnable {

  class None implements DaemonCallback {
    @Override
    public void run() {

    }
  }

}
