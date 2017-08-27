package com.liveramp.daemon_lib.serialization.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import com.liveramp.daemon_lib.JobletConfig;

public class TestGsonSerialization {

  @Test
  public void testRoundTrip() {
    final GsonSerializer<Config> ser = new GsonSerializer<>(Config.class);
    final GsonDeserializer<Config> des = new GsonDeserializer<>();


    final Config expected = new Config(ImmutableMap.of("a", "b"), Lists.newArrayList(
        ImmutableMap.of("a", "b"),
        ImmutableMap.of("d", "e")
    ),
     1, 2.0);

    final Config actual = des.apply(ser.apply(expected));
    org.junit.Assert.assertEquals(expected, actual);
  }

  public static class Config implements JobletConfig {
    private Map<String, String> map;
    private List<Map<String, String>> maps;
    private int a;
    private double b;

    private Config(ImmutableMap<String, String> map, ArrayList<Map<String, String>> maps, int a, double b) {
      this.map = map;
      this.maps = maps;
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Config) {
        final Config that = (Config)o;
        return new EqualsBuilder()
            .append(map, that.map)
            .append(maps, that.maps)
            .append(a, that.a)
            .append(b, that.b)
            .build();
      } else {
        return false;
      }
    }
  }


}