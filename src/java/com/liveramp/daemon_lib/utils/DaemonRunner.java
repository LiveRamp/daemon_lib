package com.liveramp.daemon_lib.utils;

import com.liveramp.daemon_lib.Daemon;
import com.liveramp.liveramp_hlib.util.SocketEnsureOneProcessInstance;

public class DaemonRunner {
  public static void run(Daemon daemon) throws Exception {
    new SingleDaemonProcess(daemon).ensure();
  }

  private static class SingleDaemonProcess extends SocketEnsureOneProcessInstance {
    private final Daemon daemon;

    public SingleDaemonProcess(Daemon daemon) {
      super(daemon.getIdentifier());
      this.daemon = daemon;
    }

    @Override
    protected void run() throws Exception {
      Runtime.getRuntime().addShutdownHook(new ShutdownHandler(daemon));
      daemon.start();
    }

    private class ShutdownHandler extends Thread {
      private final Daemon daemon;

      public ShutdownHandler(Daemon daemon) {
        this.daemon = daemon;
      }

      @Override
      public void run() {
        daemon.stop();
      }
    }
  }
}
