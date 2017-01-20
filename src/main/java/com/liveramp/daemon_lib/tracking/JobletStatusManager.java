package com.liveramp.daemon_lib.tracking;

public interface JobletStatusManager {
  void start(String identifier);

  void complete(String identifier);

  JobletStatus getStatus(String identifier);

  boolean exists(String identifier);

  void remove(String identifier);
}
