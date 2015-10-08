package com.liveramp.daemon_lib.tracking;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.liveramp.commons.Accessors;

public class DefaultJobletStatusManager implements JobletStatusManager {
  private final File workingDir;

  public DefaultJobletStatusManager(String workingDirectory) throws IOException {
    workingDir = new File(workingDirectory, "job_statuses");
    FileUtils.forceMkdir(workingDir);
  }

  @Override
  public void start(String identifier) {
    updateStatus(identifier, JobletStatus.IN_PROGRESS);
  }

  @Override
  public void complete(String identifier) {
    updateStatus(identifier, JobletStatus.DONE);
  }

  @Override
  public JobletStatus getStatus(String identifier) {
    File data = getFile(identifier);
    try {
      return JobletStatus.valueOf(Accessors.first(IOUtils.readLines(FileUtils.openInputStream(data))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(String identifier) {
    return getFile(identifier).exists();
  }

  @Override
  public void remove(String identifier) {
    try {
      FileUtils.forceDelete(getFile(identifier));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateStatus(String identifier, JobletStatus status) {
    File data = getFile(identifier);
    data.delete();
    try (PrintWriter out = new PrintWriter(data)) {
      out.write(status.name());
    } catch (FileNotFoundException e) {
      // This should never happen
      throw new RuntimeException(e);
    }
  }

  private File getFile(String identifier) {
    return new File(workingDir, identifier);
  }
}
