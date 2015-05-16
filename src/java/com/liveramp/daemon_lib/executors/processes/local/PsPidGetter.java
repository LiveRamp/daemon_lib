package com.liveramp.daemon_lib.executors.processes.local;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.google.common.collect.Maps;

public class PsPidGetter implements PidGetter {

  @Override
  public Map<Integer, PidData> getPids() throws IOException {
    Map<Integer, PidData> pidToCommand = Maps.newHashMap();
    Process p = Runtime.getRuntime().exec("ps -Aopid,command");
    BufferedReader inputStream = new BufferedReader(new InputStreamReader(p.getInputStream()));

    try {
      boolean first = true;
      String line;
      while ((line = inputStream.readLine()) != null) {
        String[] split = line.trim().split(" ", 2);
        if (split.length != 2) {
          throw new IOException("Unable to read pids and commands: \"" + line + "\"");
        }
        if (!first) {
          PidData pidData = new PidData();
          pidData.command = split[1].trim();
          pidToCommand.put(Integer.parseInt(split[0]), pidData);
        }
        first = false;
      }
      p.waitFor();
      return pidToCommand;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      inputStream.close();
      p.destroy();
    }
  }

  public static void main(String[] args) throws IOException {
    PsPidGetter pidGetter = new PsPidGetter();
    System.out.println(pidGetter.getPids());
  }
}
