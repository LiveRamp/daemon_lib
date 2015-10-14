package com.liveramp.daemon_lib;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;

import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.java_support.constants.ZkConstants;

public class ZKDaemonLock implements DaemonLock {

  private static final String ZK_DAEMON_LOCK_BASE_PATH = "/daemon_lib_zk/zk_daemon_locks/";

  public DaemonLock production(String daemonId) {

    CuratorFramework productionFramework = CuratorFrameworkFactory.newClient(ZkConstants.LIVERAMP_ZK_CONNECT_STRING,
        (int)TimeUnit.SECONDS.toMillis(30), (int)TimeUnit.SECONDS.toMillis(5), new RetryNTimes(3, 100));
    productionFramework.start();
    Runtime.getRuntime().addShutdownHook(new Thread(new FrameworkShutdown(productionFramework)));
    return new ZKDaemonLock(productionFramework, ZK_DAEMON_LOCK_BASE_PATH + daemonId);
  }

  private static class FrameworkShutdown implements Runnable {

    CuratorFramework fw;

    public FrameworkShutdown(CuratorFramework fw) {
      this.fw = fw;
    }

    @Override
    public void run() {
      fw.close();
    }
  }


  private final CuratorFramework framework;
  private final InterProcessMutex lock;

  public ZKDaemonLock(CuratorFramework framework, String lockPath) {
    this.framework = framework;
    this.lock = new InterProcessMutex(framework, lockPath);
  }

  @Override
  public void lock() throws DaemonException {
    try {
      lock.acquire();
    } catch (Exception e) {
      throw new DaemonException(e);
    }
  }

  @Override
  public void unlock() {
    try {
      lock.release();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
