package com.liveramp.daemon_lib.executors.processes.local;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.executors.processes.ProcessController;
import com.liveramp.daemon_lib.executors.processes.ProcessControllerException;
import com.liveramp.daemon_lib.executors.processes.ProcessDefinition;
import com.liveramp.daemon_lib.executors.processes.ProcessMetadata;


public class LocalProcessController<T extends ProcessMetadata> extends Thread implements ProcessController<T> {
  private static Logger LOG = LoggerFactory.getLogger(LocalProcessController.class);

  private final FsHelper fsHelper;
  private final ProcessHandler<T> processHandler;
  private final PidGetter pidGetter;
  private final int pollDelay;
  private final ProcessMetadata.Serializer<T> metadataSerializer;
  private boolean stop;

  private volatile List<ProcessDefinition<T>> currentProcesses;

  public LocalProcessController(FsHelper fsHelper, ProcessHandler<T> processHandler, PidGetter pidGetter, int pollDelay, ProcessMetadata.Serializer<T> metadataSerializer) {
    super(LocalProcessController.class.getSimpleName());
    this.fsHelper = fsHelper;
    this.processHandler = processHandler;
    this.pidGetter = pidGetter;
    this.pollDelay = pollDelay;
    this.metadataSerializer = metadataSerializer;
    this.stop = false;
    this.currentProcesses = Lists.newLinkedList();

    setDaemon(true);
  }

  @Override
  public void registerProcess(int pid, T metadata) throws ProcessControllerException {
    LOG.info("Registering process {}.", pid);
    ProcessDefinition<T> process = new ProcessDefinition<T>(pid, metadata);
    File tmpFile = fsHelper.getPidTmpPath(process.getPid());
    try {
      fsHelper.writeMetadata(tmpFile, metadataSerializer.toBytes(process.getMetadata()));
    } catch (IOException e) {
      throw new ProcessControllerException(String.format("Unable to create control file '%s' for %d pid.", tmpFile.toString(), process.getPid()), e);
    }
    File pidFile = fsHelper.getPidPath(process.getPid());
    if (!tmpFile.renameTo(pidFile)) {
      throw new ProcessControllerException(String.format("Unable to commit control file '%s' for %d pid.", pidFile.toString(), process.getPid()));
    }
    processHandler.onAdd(process);
  }

  @Override
  public List<ProcessDefinition<T>> getProcesses() throws ProcessControllerException {
    return currentProcesses;
  }

  @Override
  public void run() {
    while (!stop) {
      try {
        List<ProcessDefinition<T>> watchedProcesses = getWatchedProcesses(fsHelper);
        Map<Integer, PidGetter.PidData> runningPids = pidGetter.getPids();
        Iterator<ProcessDefinition<T>> iterator = watchedProcesses.iterator();
        while (iterator.hasNext()) {
          ProcessDefinition watchedProcess = iterator.next();
          if (!runningPids.containsKey(watchedProcess.getPid())) {
            LOG.info("Deregister process {}.", watchedProcess.getPid());
            File watchedFile = fsHelper.getPidPath(watchedProcess.getPid());
            processHandler.onRemove(watchedProcess);
            watchedFile.delete();
            iterator.remove();
          }
        }
        currentProcesses = watchedProcesses;
      } catch (Exception e) {
        LOG.warn("Exception while watching processes.", e);
      }
      doSleep(pollDelay);
    }
  }

  private void doSleep(long pollDelay) {
    try {
      Thread.sleep(pollDelay);
    } catch (InterruptedException e) {
      LOG.error("Error", e);
    }
  }

  private List<ProcessDefinition<T>> getWatchedProcesses(FsHelper fsHelper) throws IOException {
    List<ProcessDefinition<T>> pids = Lists.newLinkedList();
    String[] fileList = fsHelper.getBasePath().list();
    if (fileList != null) {
      for (String s : fileList) {
        if (s.matches("\\d+")) {
          int pid = Integer.parseInt(s);
          File pidPath = fsHelper.getPidPath(pid);
          ProcessDefinition<T> process = new ProcessDefinition<T>(pid, metadataSerializer.fromBytes(fsHelper.readMetadata(pidPath)));
          pids.add(process);
        }
      }
    }

    return pids;
  }
}
