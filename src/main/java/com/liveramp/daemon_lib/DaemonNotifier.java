package com.liveramp.daemon_lib;


import java.util.Optional;

public interface DaemonNotifier {

  void notify(String subject, Optional<String> body, Optional<? extends Throwable> t);
}

