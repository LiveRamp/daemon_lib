package com.liveramp.daemon_lib.executors.processes;

public class ProcessControllerException extends Exception {
  public ProcessControllerException() {
    super();
  }

  public ProcessControllerException(String message) {
    super(message);
  }

  public ProcessControllerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProcessControllerException(Throwable cause) {
    super(cause);
  }
}
