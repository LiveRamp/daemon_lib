package com.liveramp.daemon_lib.utils;

/**
 * Indicates a timeout when trying to get a lock
 */
public class LockTimeoutException extends DaemonException {

  LockTimeoutException(String msg) {
    super(msg);
  }
}
