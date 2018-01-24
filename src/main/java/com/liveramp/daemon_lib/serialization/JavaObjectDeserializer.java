package com.liveramp.daemon_lib.serialization;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.function.Function;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectStreamClass;


import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.daemon_lib.JobletConfig;

public class JavaObjectDeserializer<T extends JobletConfig> implements Function<byte[], T> {

  private static Logger LOG = LoggerFactory.getLogger(JavaObjectDeserializer.class);

  @SuppressWarnings("unchecked")
  @Override
  public T apply(byte[] bytes) {
    try {
      return (T)SerializationUtils.deserialize(bytes);
    } catch (SerializationException e) {
      LOG.error("Error during deserialization:", e);
      LOG.error("Failed to deserialize the joblet config due to some serialization error. " +
          "Attempting again while ignoring serial version UID - this may allow some cases to recover, " +
          "but may also lead to fatal errors later in the log if the class changes are in fact incompatible. " +
          "Use a non-default serializer!");
      //Attempt again, this time ignoring serial version id
      try {
        IgnoreSerialVersionUIDStream stream = new IgnoreSerialVersionUIDStream(new ByteArrayInputStream(bytes));
        Object result = stream.readObject();
        stream.close();
        return (T)result;
      } catch (IOException | ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }
  }


  //Code taken from stackoverflow: https://stackoverflow.com/questions/1816559/make-java-runtime-ignore-serialversionuids
  public static class IgnoreSerialVersionUIDStream extends ObjectInputStream {

    private static Logger LOG = LoggerFactory.getLogger(IgnoreSerialVersionUIDStream.class);

    IgnoreSerialVersionUIDStream(InputStream in) throws IOException {
      super(in);
    }

    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
      ObjectStreamClass resultClassDescriptor = super.readClassDescriptor(); // initially streams descriptor
      Class localClass; // the class in the local JVM that this descriptor represents.
      try {
        localClass = Class.forName(resultClassDescriptor.getName());
      } catch (ClassNotFoundException e) {
        LOG.error("No local class for " + resultClassDescriptor.getName(), e);
        return resultClassDescriptor;
      }
      ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
      if (localClassDescriptor != null) { // only if class implements serializable
        final long localSUID = localClassDescriptor.getSerialVersionUID();
        final long streamSUID = resultClassDescriptor.getSerialVersionUID();
        if (streamSUID != localSUID) { // check for serialVersionUID mismatch.
          final StringBuffer s = new StringBuffer("Overriding serialized class version mismatch: ");
          s.append("local serialVersionUID = ").append(localSUID);
          s.append(" stream serialVersionUID = ").append(streamSUID);
          Exception e = new InvalidClassException(s.toString());
          LOG.error("Potentially Fatal Deserialization Operation.", e);
          resultClassDescriptor = localClassDescriptor; // Use local class descriptor for deserialization
        }
      }
      return resultClassDescriptor;
    }
  }
}
