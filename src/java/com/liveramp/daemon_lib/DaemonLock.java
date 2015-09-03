package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface DaemonLock {

  //Blocks until the lock is acquired
  void lock() throws DaemonException;

  void unlock() throws DaemonException;


}
