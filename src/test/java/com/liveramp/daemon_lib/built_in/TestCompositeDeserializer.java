package com.liveramp.daemon_lib.built_in;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCompositeDeserializer {

  @Test
  public void testFallsBackToFirstSuccessfulDeserializer() {
    final Charset charset = Charset.forName("UTF-8");
    final Set<String> expected = Collections.singleton("Hello");
    final byte[] bytes = new Gson().toJson(expected).getBytes(charset);
    final CompositeDeserializer<Set<String>> deser = new CompositeDeserializer<>(Lists.newArrayList(
        b -> {
          throw new RuntimeException();
        },
        b -> new Gson().fromJson(new String(bytes, charset), Set.class),
        b -> Collections.singleton("world")
    ));

    final Set<String> actualSet = deser.apply(bytes);
    assertEquals(expected, actualSet);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsEntirelyIfNoSerializerWorks() {
    final Charset charset = Charset.forName("UTF-8");
    final Set<String> expected = Collections.singleton("Hello");
    final byte[] bytes = new Gson().toJson(expected).getBytes(charset);
    final CompositeDeserializer<Set<String>> deser = new CompositeDeserializer<>(Lists.newArrayList(
        b -> {
          throw new RuntimeException();
        }
    ));

    deser.apply(bytes);
  }

  @Test
  public void testCompositeOfComposites() {
    final Charset charset = Charset.forName("UTF-8");
    final IDConfig expected = new IDConfig(1L);
    final byte[] bytes = new Gson().toJson(expected).getBytes(charset);
    final CompositeDeserializer<IDConfig> deser = new CompositeDeserializer<>(Lists.newArrayList(
        b -> {
          throw new RuntimeException();
        },
        b -> new Gson().fromJson(new String(bytes, charset), IDConfig.class),
        b -> new IDConfig(1L)
    ));
    final CompositeDeserializer<IDConfig> compcomp = new CompositeDeserializer<IDConfig>(Lists.newArrayList(
        b -> { throw new RuntimeException(); },
        deser
    ));

    final IDConfig actual = compcomp.apply(bytes);
    assertEquals(expected.getId(), actual.getId());
  }

}
