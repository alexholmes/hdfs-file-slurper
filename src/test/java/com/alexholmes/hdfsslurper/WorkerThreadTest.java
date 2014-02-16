package com.alexholmes.hdfsslurper;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class WorkerThreadTest {

  static {
    ((Log4JLogger) LogFactory.getLog("org")
    ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger) LogFactory.getLog("ipc")
    ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger) LogFactory.getLog("datanode")
    ).getLogger().setLevel(Level.OFF);
  }

  private static String TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data", "/tmp")).toString().replace(' ', '+');

  private static final Random RAN = new Random();

  public static class TestFile {
    protected Path path;
    private final FileSystem fs;
    private long crc;

    public TestFile(FileSystem fs, Path path) throws IOException {
      this.fs = fs;
      this.path = path;
      create();
    }

    private void create() throws IOException {
      FSDataOutputStream out = fs.create(path);
      try {
        byte[] b = new byte[1024 + RAN.nextInt(1024)];
        RAN.nextBytes(b);
        out.write(b);
      } finally {
        if (out != null) out.close();
      }

      crc = hdfsFileCRC32(fs, null, path);
    }

    public long getCRC32() {
      return crc;
    }
  }

  public static long hdfsFileCRC32(FileSystem fs, CompressionCodec codec, Path path) throws IOException {
    InputStream in = null;
    CRC32 crc = new CRC32();
    try {
      InputStream is = new BufferedInputStream(fs.open(path));
      if (codec != null) {
        is = codec.createInputStream(is);
      }
      in = new CheckedInputStream(is, crc);
      org.apache.commons.io.IOUtils.copy(in, new NullOutputStream());
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(in);
    }
    return crc.getValue();
  }

  @Test
  public void testCopyFromLocalToDfs() throws Exception {
    Configuration conf = new Configuration();
    final FileSystem srcFs = FileSystem.getLocal(conf);
    final FileSystem destFs = FileSystem.getLocal(conf);
    final String namenode = destFs.getUri().toString();
    System.out.println("NN = " + namenode);
    if (namenode.startsWith("hdfs://")) {

      Path localBaseDir = srcFs.makeQualified(new Path(TEST_ROOT_DIR, "test-slurper"));

      System.out.println(localBaseDir.toString());


      Path localInDir = new Path(localBaseDir, "in");
      Path localWorkDir = new Path(localBaseDir, "work");
      Path localErrorDir = new Path(localBaseDir, "error");
      Path localCompleteDir = new Path(localBaseDir, "completed");

      srcFs.delete(localBaseDir, true);

      srcFs.mkdirs(localInDir);

      Path localInFile = new Path(localInDir, "test-file");
      TestFile inFile = new TestFile(srcFs, localInFile);


      Path hdfsBaseDir = destFs.makeQualified(new Path("/tmp", "test-slurper"));
      Path hdfsDestDir = new Path(hdfsBaseDir, "out");
      Path hdfsStageDir = new Path(hdfsBaseDir, "stage");

      System.out.println(hdfsBaseDir.toString());

      Config c = new Config();
      c.setSrcDir(localInDir)
          .setWorkDir(localWorkDir)
          .setErrorDir(localErrorDir)
          .setCompleteDir(localCompleteDir)
          .setDestDir(hdfsDestDir)
          .setDestStagingDir(hdfsStageDir)
          .setPollSleepPeriodMillis(1000)
          .setSrcFs(srcFs)
          .setDestFs(destFs);


      FileSystemManager fsm = new FileSystemManager(c);

      WorkerThread wt = new WorkerThread(c, fsm, TimeUnit.MILLISECONDS, 1);

      wt.doWork();

      Path expectedDestinationFile = new Path(hdfsDestDir, localInFile.getName());

      assertTrue("File doesn't exist '" + expectedDestinationFile.toUri() + "'", destFs.exists(expectedDestinationFile));

      assertEquals(inFile.getCRC32(), hdfsFileCRC32(destFs, null, expectedDestinationFile));
    }
  }


}
