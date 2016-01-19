package com.liveramp.daemon_lib.utils;

import com.google.common.base.Optional;

import com.liveramp.daemon_lib.DaemonNotifier;

public class NoOpDaemonNotifier implements DaemonNotifier {
  public void notify(String subject, Optional<String> body, Optional<? extends Throwable> t) {}
}
