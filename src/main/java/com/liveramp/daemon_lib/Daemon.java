package com.liveramp.daemon_lib;

import com.google.common.base.Optional;

import com.liveramp.daemon_lib.executors.JobletExecutor;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.postconfig.ConfigBasedExecutionCondition;
import com.liveramp.daemon_lib.executors.processes.execution_conditions.preconfig.ExecutionCondition;
import com.liveramp.daemon_lib.utils.DaemonException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import com.liveramp.daemon_lib.utils.HostUtil;

public class Daemon<T extends JobletConfig, I> {
  private static final Logger LOG = LoggerFactory.getLogger(Daemon.class);

  private static final int DEFAULT_CONFIG_WAIT_SECONDS = 1;
  private static final int DEFAULT_EXECUTION_SLOT_WAIT_SECONDS = 0;
  private static final int DEFAULT_NEXT_CONFIG_WAIT_SECONDS = 0;
  private static final int DEFAULT_FAILURE_WAIT_SECONDS = 10;

  public static class Options {
    private int configWaitSeconds = DEFAULT_CONFIG_WAIT_SECONDS;
    private int executionSlotWaitSeconds = DEFAULT_EXECUTION_SLOT_WAIT_SECONDS;
    private int nextConfigWaitSeconds = DEFAULT_NEXT_CONFIG_WAIT_SECONDS;
    private int failureWaitSeconds = DEFAULT_FAILURE_WAIT_SECONDS;

    /**
     * @param sleepingSeconds How long the daemon should wait before retrying when there is no config available.
     *                        Please avoid setting this below the default value of 1, this typically leads to
     *                        spinning on the database, or whatever you're checking for new configs.  In the worst case,
     *                        setting this to 1 should delay all work by at most 1 second (i.e. add 1 second to your SLAs).
     * @return options for fluent usage
     */
    public Options setConfigWaitSeconds(int sleepingSeconds) {
      this.configWaitSeconds = sleepingSeconds;
      return this;
    }

    /**
     * @param sleepingSeconds How long the daemon should wait before retrying when the max number of running joblets is reached.
     * @return options for fluent usage
     */
    public Options setExecutionSlotWaitSeconds(int sleepingSeconds) {
      this.executionSlotWaitSeconds = sleepingSeconds;
      return this;
    }

    /**
     * @param sleepingSeconds How long the daemon should wait before fetching the next config.
     * @return options for fluent usage
     */
    public Options setNextConfigWaitSeconds(int sleepingSeconds) {
      this.nextConfigWaitSeconds = sleepingSeconds;
      return this;
    }

    /**
     * @param sleepingSeconds How long the daemon should wait before retrying when it did not successfully execute a config.
     * @return options for fluent usage
     */
    public Options setFailureWaitSeconds(int sleepingSeconds) {
      this.failureWaitSeconds = sleepingSeconds;
      return this;
    }
  }

  private final String identifier;
  private final JobletExecutor<T> executor;
  private final DaemonNotifier notifier;
  private final JobletConfigProducer<T> configProducer;

  private final Options options;

  private boolean running;
  private final JobletCallback<? super T> preExecutionCallback;
  private DaemonLock lock;
  private final ExecutionCondition executionCondition;
  private final ConfigBasedExecutionCondition<T> configBasedExecutionCondition;

  public Daemon(String identifier, JobletExecutor<T> executor, JobletConfigProducer<T> configProducer, JobletCallback<? super T> preExecutionCallback, DaemonLock lock, DaemonNotifier notifier, Options options, ExecutionCondition executionCondition, ConfigBasedExecutionCondition<T> configBasedExecutionCondition) {
    this.preExecutionCallback = preExecutionCallback;
    this.executionCondition = executionCondition;
    this.configBasedExecutionCondition = configBasedExecutionCondition;
    this.identifier = clean(identifier);
    this.configProducer = configProducer;
    this.executor = executor;
    this.notifier = notifier;
    this.options = options;
    this.lock = lock;

    this.running = false;
  }

  private static String clean(String identifier) {
    return identifier.replaceAll("\\s", "-");
  }

  public final void start() {
    LOG.info("Running open-source version of daemon_lib");
    LOG.info("Starting daemon {}", getDaemonSignature());
    running = true;

    try {
      while (running) {
        if (!processNext()) {
          silentSleep(options.failureWaitSeconds);
        }
        silentSleep(options.nextConfigWaitSeconds);
      }
    } catch (Exception e) {
      notifier.notify("Fatal error occurred in daemon (" + getDaemonSignature() + "). Shutting down.", Optional.<String>absent(), Optional.of(e));
      throw e;
    }
    LOG.info("Exiting daemon ({})", getDaemonSignature());
  }

  protected boolean processNext() {
    if (executionCondition.canExecute()) {
      T jobletConfig;
      try {
        lock.lock();
        jobletConfig = configProducer.getNextConfig();
      } catch (DaemonException e) {
        notifier.notify("Error getting next config for daemon (" + getDaemonSignature() + ")", Optional.<String>absent(), Optional.of(e));
        lock.unlock();
        return false;
      }
      String initValue;
      if (jobletConfig != null && configBasedExecutionCondition.apply(jobletConfig)) {
        LOG.info("Found joblet config: " + jobletConfig);
        try {
          initValue = executor.initialize(jobletConfig);
          preExecutionCallback.callback(jobletConfig);
        } catch (DaemonException e) {
          notifier.notify("Error executing callbacks for daemon (" + getDaemonSignature() + ")",
              Optional.of(jobletConfig.toString() + "\n" + preExecutionCallback.toString()),
              Optional.of(e));
          return false;
        } finally {
          lock.unlock();
        }
        try {
          executor.execute(initValue, jobletConfig);
        } catch (Exception e) {
          notifier.notify("Error executing joblet config for daemon (" + getDaemonSignature() + ")",
              Optional.of(jobletConfig.toString()),
              Optional.of(e));
          return false;
        }
      } else {
        lock.unlock();
        silentSleep(options.configWaitSeconds);
      }
    } else {
      silentSleep(options.executionSlotWaitSeconds);
    }

    return true;
  }

  public final void stop() {
    running = false;
    executor.shutdown();
  }

  public String getIdentifier() {
    return identifier;
  }

  private void silentSleep(int seconds) {
    try {
      if (seconds > 0) {
        TimeUnit.SECONDS.sleep(seconds);
      }
    } catch (InterruptedException e) {
      LOG.error("Daemon interrupted: ", e);
    }
  }

  private String getDaemonSignature() {
    final String hostName = HostUtil.safeGetHostName();
    return String.format("(%s) on (%s)", identifier, hostName);
  }

}
