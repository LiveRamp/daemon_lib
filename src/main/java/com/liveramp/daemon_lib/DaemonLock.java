package com.liveramp.daemon_lib;

import com.liveramp.daemon_lib.utils.DaemonException;

public interface DaemonLock {

  //Blocks until the lock is acquired
  void lock() throws DaemonException;

  //Unlock the lock - should be idempotent!
  //If something prevents the unlock, this method should throw an Unchecked exception
  //to ensure that the process is killed and locks are released by the lock management framework
  void unlock();


}
