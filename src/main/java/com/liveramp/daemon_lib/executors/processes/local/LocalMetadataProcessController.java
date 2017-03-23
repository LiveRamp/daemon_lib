package com.liveramp.daemon_lib.executors.processes.local;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.DaemonNotifier;
import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.executors.processes.ProcessControllerException;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;
import com.liveramp.daemon_lib.utils.DaemonException;
import com.liveramp.daemon_lib.utils.HostUtil;


public class LocalMetadataProcessController<T extends ProcessMetadata, Pid> implements ProcessController<T, Pid> {
  private static Logger LOG = LoggerFactory.getLogger(LocalMetadataProcessController.class);

  private final DaemonNotifier notifier;
  private final FsHelper fsHelper;
  private FileNamePidProcessor<Pid> pidProcessor;
  private final ProcessHandler<T, Pid> processHandler;
  private final RunningProcessGetter<Pid, ?, T> runningProcessGetter;
  private final ProcessMetadata.Serializer<T> metadataSerializer;

  private volatile List<ProcessDefinition<T, Pid>> currentProcesses;

  public LocalMetadataProcessController(DaemonNotifier notifier, FsHelper fsHelper, FileNamePidProcessor<Pid> pidProcessor, ProcessHandler<T, Pid> processHandler, RunningProcessGetter<Pid, ?, T> runningProcessGetter, int pollDelay, ProcessMetadata.Serializer<T> metadataSerializer) {
    this.notifier = notifier;
    this.fsHelper = fsHelper;
    this.pidProcessor = pidProcessor;
    this.processHandler = processHandler;
    this.runningProcessGetter = runningProcessGetter;
    this.metadataSerializer = metadataSerializer;
    this.currentProcesses = null;

    Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("process watcher")
            .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
            .build()
    ).scheduleWithFixedDelay(new ProcessesWatcher(), 0, pollDelay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void registerProcess(Pid pid, T metadata) throws ProcessControllerException {
    LOG.info("Registering process {}.", pid);
    ProcessDefinition<T, Pid> process = new ProcessDefinition<>(pid, metadata);
    File tmpFile = fsHelper.getPidTmpPath(process.getPid());
    if (!tmpFile.getParentFile().exists() && !tmpFile.getParentFile().mkdirs()) {
      throw new ProcessControllerException(String.format("Unable to create parent directory '%s' for %d pid", tmpFile.getParent(), process.getPid()));
    }
    try {
      fsHelper.writeMetadata(tmpFile, metadataSerializer.toBytes(process.getMetadata()));
    } catch (IOException e) {
      throw new ProcessControllerException(String.format("Unable to create control file '%s' for %d pid.", tmpFile.toString(), process.getPid()), e);
    }
    File pidFile = fsHelper.getPidPath(process.getPid());
    if (!tmpFile.renameTo(pidFile)) {
      throw new ProcessControllerException(String.format("Unable to commit control file '%s' for %d pid.", pidFile.toString(), process.getPid()));
    }
  }

  @Override
  public List<ProcessDefinition<T, Pid>> getProcesses() throws ProcessControllerException {
    try {
      return getWatchedProcesses(fsHelper);
    } catch (IOException e) {
      throw new ProcessControllerException(e);
    }
  }

  private class ProcessesWatcher implements Runnable {
    @Override
    public void run() {
      try {
        List<ProcessDefinition<T, Pid>> watchedProcesses = getWatchedProcesses(fsHelper);
        Map<Pid, ?> runningPids = runningProcessGetter.getPids(watchedProcesses);
        Iterator<ProcessDefinition<T, Pid>> iterator = watchedProcesses.iterator();
        while (iterator.hasNext()) {
          ProcessDefinition<T, Pid> watchedProcess = iterator.next();
          if (!runningPids.containsKey(watchedProcess.getPid())) {
            LOG.info("Deregister process {}.", watchedProcess.getPid());
            File watchedFile = fsHelper.getPidPath(watchedProcess.getPid());
            try {
              processHandler.onRemove(watchedProcess);
            } catch (DaemonException e) {
              LOG.error("Exception while handling process termination.", e);
              notifier.notify(
                  String.format("Error handling joblet termination in daemon for joblet with pid %s on %s", watchedProcess.getPid(), HostUtil.safeGetHostName()),
                  Optional.of(String.format("Configuration: %s. Exception:%s", watchedProcess.getMetadata(), ExceptionUtils.getStackTrace(e))),
                  Optional.<Throwable>absent()
              );
            }
            watchedFile.delete();
            iterator.remove();
          }
        }
      } catch (Exception e) {
        LOG.warn("Exception while watching processes.", e);
      }
    }
  }

  private synchronized List<ProcessDefinition<T, Pid>> getWatchedProcesses(FsHelper fsHelper) throws IOException {
    List<ProcessDefinition<T, Pid>> pids = Lists.newLinkedList();
    String[] fileList = fsHelper.getBasePath().list();
    if (fileList != null) {
      for (String s : fileList) {
        Optional<Pid> pidOptional = pidProcessor.processFileName(s);
        if (pidOptional.isPresent()) {
          File pidPath = fsHelper.getPidPath(pidOptional.get());
          ProcessDefinition<T, Pid> process = new ProcessDefinition<>(pidOptional.get(), metadataSerializer.fromBytes(fsHelper.readMetadata(pidPath)));
          pids.add(process);
        }
      }
    }
    currentProcesses = pids;
    return currentProcesses;
  }
}
