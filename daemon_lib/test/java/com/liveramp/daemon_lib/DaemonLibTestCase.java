package com.liveramp.daemon_lib;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

public class DaemonLibTestCase {
  protected String testName;

  static {
    // this prevents the default log4j.properties (hidden inside the hadoop jar)
    // from being loaded automatically.
    System.setProperty("log4j.defaultInitOverride", "true");
  }
  protected static final String SEPARATOR = "--------------------";

  public DaemonLibTestCase() {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

    rootLogger.setLevel(Level.ALL);
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(Level.INFO);
    org.apache.log4j.Logger.getLogger("cascading").setLevel(Level.INFO);
    org.apache.log4j.Logger.getLogger("org.eclipse.jetty").setLevel(Level.ERROR);

    testName = this.getClass().getSimpleName(); // Set the test's name

    rootLogger.setLevel(Level.ALL);

    // Reconfigure the logger to ensure things are working

    ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"), ConsoleAppender.SYSTEM_ERR);
    consoleAppender.setName("test-console-appender");
    consoleAppender.setFollow(true);

    rootLogger.removeAppender("test-console-appender");
    rootLogger.addAppender(consoleAppender);
  }
}
