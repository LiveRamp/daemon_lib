package com.liveramp.daemon_lib.utils;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.DaemonNotifier;

public class LoggingForwardingNotifier implements DaemonNotifier {

  private static Logger LOG = LoggerFactory.getLogger(LoggingForwardingNotifier.class);

  private final DaemonNotifier delegate;

  public LoggingForwardingNotifier(DaemonNotifier delegate) {
    this.delegate = delegate;
  }

  @Override
  public void notify(String subject, Optional<String> body, Optional<? extends Throwable> t) {

    String message = String.format("%s\n%s", subject, body.orElse(""));

    if (t.isPresent()) {
      LOG.error(message, t.get());
    } else {
      LOG.error(message);
    }
    delegate.notify(subject, body, t);
  }
}
