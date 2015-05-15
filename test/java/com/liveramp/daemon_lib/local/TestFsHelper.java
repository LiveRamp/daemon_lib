package com.liveramp.daemon_lib.local;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFsHelper {
  private final String PATH = "/tmp/tests/s2s_dispatcher/" + TestFsHelper.class.getName() + "_AUTOGEN";
  FsHelper fsHelper;

  @Before
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(new File(PATH));
    new File(PATH).mkdirs();
    fsHelper = new FsHelper(PATH);
  }

  @Test
  public void testIt() throws Exception {
    assertEquals(PATH, fsHelper.getBasePath().toString());
    assertEquals(PATH + "/1", fsHelper.getPidPath(1).toString());
    assertEquals(PATH + "/1_tmp", fsHelper.getPidTmpPath(1).toString());
    byte[] metadata = new byte[]{0x1, 0x2, 0x3, 0x4, 0x5};
    fsHelper.writeMetadata(fsHelper.getPidPath(1), metadata);
    System.out.println(Arrays.toString(fsHelper.readMetadata(fsHelper.getPidPath(1))));
    assertTrue(Arrays.equals(metadata, fsHelper.readMetadata(fsHelper.getPidPath(1))));
  }
}
