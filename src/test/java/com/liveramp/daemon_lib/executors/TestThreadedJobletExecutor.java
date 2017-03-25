package com.liveramp.daemon_lib.executors;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.daemon_lib.DaemonLibTestCase;
import com.liveramp.daemon_lib.Joblet;
import com.liveramp.daemon_lib.JobletCallback;
import com.liveramp.daemon_lib.JobletFactory;
import com.liveramp.daemon_lib.built_in.IDConfig;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.DefaultThreadedExecutionCondition;
import com.liveramp.daemon_lib.utils.DaemonException;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestThreadedJobletExecutor extends DaemonLibTestCase {

  private ThreadPoolExecutor pool;
  private JobletFactory<IDConfig> factory;
  private ThreadedJobletExecutor<IDConfig> jobletExecutor;
  private JobletCallback<IDConfig> successCallback;
  private JobletCallback<IDConfig> failureCallback;

  @Before
  public void setUp() throws Exception {
    pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(2);
    factory = mock(JobletFactory.class, RETURNS_DEEP_STUBS);
    successCallback = mock(JobletCallback.class);
    failureCallback = mock(JobletCallback.class);
    jobletExecutor = new ThreadedJobletExecutor<>(pool, factory, successCallback, failureCallback, () -> new ThreadedJobletExecutor.Config(2));
  }

  @After
  public void tearDown() throws Exception {
    pool.shutdownNow();
  }

  @Test
  public void testExecuteJoblet() throws Exception {
    IDConfig config = new IDConfig(1);

    jobletExecutor.execute(config);

    pool.shutdown();
    pool.awaitTermination(10, TimeUnit.SECONDS);

    verify(factory.create(config), times(1)).run();
    verify(successCallback, times(1)).callback(config);

    verify(failureCallback, times(0)).callback(config);
  }

  @Test
  public void testExecuteJobletExceptionCallAfter() throws Exception {
    IDConfig config = new IDConfig(1);

    when(factory.create(config)).thenReturn(new Joblet() {
      @Override
      public void run() throws DaemonException {
        throw new DaemonException();
      }
    });

    jobletExecutor.execute(config);

    pool.shutdown();
    pool.awaitTermination(10, TimeUnit.SECONDS);

    verify(failureCallback, times(1)).callback(config);

    verify(successCallback, times(0)).callback(config);
  }

  @Test
  public void testLimit() throws Exception {
    IDConfig config = new IDConfig(1);

    final AtomicBoolean stop = new AtomicBoolean(false);
    when(factory.create(config)).thenReturn(new Joblet() {
      @Override
      public void run() throws DaemonException {
        while (!stop.get()) {
        }
      }
    });

    final DefaultThreadedExecutionCondition defaultThreadedExecutionCondition = new DefaultThreadedExecutionCondition(pool);

    Assert.assertTrue(defaultThreadedExecutionCondition.canExecute());
    jobletExecutor.execute(config);
    Assert.assertTrue(defaultThreadedExecutionCondition.canExecute());

    jobletExecutor.execute(config);
    Assert.assertFalse(defaultThreadedExecutionCondition.canExecute());

    stop.set(true);
    pool.shutdown();
    pool.awaitTermination(10, TimeUnit.SECONDS);

    Assert.assertTrue(defaultThreadedExecutionCondition.canExecute());
  }

}
