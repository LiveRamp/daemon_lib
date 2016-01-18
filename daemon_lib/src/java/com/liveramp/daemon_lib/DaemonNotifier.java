package com.liveramp.daemon_lib;

public interface DaemonNotifier {

  void sendAlert(String subject, String body);

  void sendAlert(String subject, Throwable t);

  void sendAlert(String subject, String body, Throwable t);
}
