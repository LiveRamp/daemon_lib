package com.liveramp.daemon_lib;

import com.google.common.base.Optional;

public class NoOpDaemonNotifier implements DaemonNotifier{
  public void notify(String subject, Optional<String> body, Optional<? extends Throwable> t) {}
}
