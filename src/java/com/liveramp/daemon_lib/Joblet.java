package com.liveramp.daemon_lib;

import java.io.Serializable;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface Joblet extends Serializable {
  public void run() throws DaemonException;

  public void afterExecution() throws DaemonException;
}
