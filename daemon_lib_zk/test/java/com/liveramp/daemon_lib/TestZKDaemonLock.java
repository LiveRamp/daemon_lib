package com.liveramp.daemon_lib;

import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingCluster;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Test;

import com.liveramp.daemon_lib.utils.DaemonException;

public class TestZKDaemonLock extends DaemonLibTestCase {

  @Test
  public void testLockUnlock() throws Exception {

    Logger logger = Logger.getLogger("org.apache.curator");
    logger.setLevel(Level.ERROR);
    logger = Logger.getLogger("org.apache.zookeeper");
    logger.setLevel(Level.ERROR);

    TestingCluster cluster = new TestingCluster(3);
    cluster.start();

    CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryNTimes(1, 10));
    client.start();

    List<Thread> threads = Lists.newArrayList();
    List<String> strings = Lists.newArrayList();
    StringBuilder out = new StringBuilder();
    String expected = "";

    for (int i = 0; i < 10; i++) {
      Thread thread = makeThread(client, strings, out);
      threads.add(thread);
      strings.add(i + "");
      expected += i + "\n";
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }


    client.close();
    cluster.close();

    System.out.println(out.toString());
    Assert.assertEquals(expected, out.toString());



  }

  public Thread makeThread(CuratorFramework client, final List<String> strings, final StringBuilder out) throws Exception {
    final ZKDaemonLock lock = new ZKDaemonLock(client, "/daemon");

    Thread thread = new Thread(
        new Runnable() {
          @Override
          public void run() {
            try {
              lock.lock();
              String s = strings.get(0);
              System.out.println(s);
              out.append(s + "\n");
              TimeUnit.MILLISECONDS.sleep(new Random().nextInt(15));
              strings.remove(0);
              lock.unlock();
            } catch (DaemonException e) {
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );
    return thread;
  }
}
