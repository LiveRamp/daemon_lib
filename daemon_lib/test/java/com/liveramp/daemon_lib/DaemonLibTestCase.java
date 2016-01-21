package com.liveramp.daemon_lib;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

public class DaemonLibTestCase {
  public DaemonLibTestCase() {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

    rootLogger.setLevel(Level.ALL);

    ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"), ConsoleAppender.SYSTEM_ERR);
    consoleAppender.setName("test-console-appender");
    consoleAppender.setFollow(true);

    rootLogger.removeAppender("test-console-appender");
    rootLogger.addAppender(consoleAppender);
  }
}
