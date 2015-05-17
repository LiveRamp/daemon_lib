package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.liveramp_hlib.util.EnsureOneProcessInstance;

public class DaemonRunner {
  public static void run(Daemon daemon) throws Exception {
    new SingleDaemonProcess(daemon).ensure();
  }

  private static class SingleDaemonProcess extends EnsureOneProcessInstance {
    private final Daemon daemon;

    public SingleDaemonProcess(Daemon daemon) {
      super(daemon.getIdentifier());
      this.daemon = daemon;
    }

    @Override
    protected void run() throws Exception {
      daemon.start();
    }
  }
}
