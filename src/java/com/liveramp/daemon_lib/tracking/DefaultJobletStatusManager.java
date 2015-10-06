package com.liveramp.daemon_lib.tracking;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class DefaultJobletStatusManager implements JobletStatusManager {
  private final Map<Object, Object> jobStatuses;
  private final DB db;

  public DefaultJobletStatusManager(String workingDirectory) throws IOException {
    File file = new File(workingDirectory, "job_statuses");
    FileUtils.forceMkdir(file.getParentFile());
    db = DBMaker
        .newFileDB(file)
        .make();

    jobStatuses = db.getHashMap("job_statuses");
  }

  @Override
  public void start(String identifier) {
    jobStatuses.put(identifier, JobletStatus.IN_PROGRESS.name());
    db.commit();
  }

  @Override
  public void complete(String identifier) {
    jobStatuses.put(identifier, JobletStatus.DONE.name());
    db.commit();
  }

  @Override
  public JobletStatus getStatus(String identifier) {
    return JobletStatus.valueOf((String)jobStatuses.get(identifier));
  }

  @Override
  public boolean exists(String identifier) {
    return jobStatuses.containsKey(identifier);
  }

  @Override
  public void remove(String identifier) {
    jobStatuses.remove(identifier);
  }
}
