package com.liveramp.daemon_lib.utils;

import java.net.InetAddress;

public class HostUtil {
  public static String safeGetHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "Unknown host";
    }
  }
}
