package com.liveramp.daemon_lib;

import java.io.Serializable;

public interface Joblet extends Serializable {
  public void run();

  public void onComplete();
}
