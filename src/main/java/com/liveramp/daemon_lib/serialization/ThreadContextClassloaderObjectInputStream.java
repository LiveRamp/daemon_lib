package com.liveramp.daemon_lib.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ThreadContextClassloaderObjectInputStream extends ObjectInputStream {

  public ThreadContextClassloaderObjectInputStream(InputStream in) throws IOException {
    super(in);
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    return Thread.currentThread().getContextClassLoader().loadClass(desc.getName());
  }
}
