package com.alexholmes.hdfsslurper;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static junit.framework.Assert.*;

public class WorkerThreadTest {

  static {
    ((Log4JLogger) LogFactory.getLog("org")
        ).getLogger().setLevel(Level.OFF);
      ((Log4JLogger) LogFactory.getLog("ipc")
          ).getLogger().setLevel(Level.OFF);
      ((Log4JLogger) LogFactory.getLog("datanode")
          ).getLogger().setLevel(Level.OFF);
  }

    static final URI LOCAL_FS = URI.create("file:///");

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
        MiniDFSCluster cluster = null;
        try {
            Configuration conf = new Configuration();
            cluster = new MiniDFSCluster(conf, 1, true, null);
            final FileSystem hdfs = cluster.getFileSystem();
            final FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
            final String namenode = hdfs.getUri().toString();
            System.out.println("NN = " + namenode);
            if (namenode.startsWith("hdfs://")) {

                Path localBaseDir = localfs.makeQualified(new Path(TEST_ROOT_DIR, "test-slurper"));

                System.out.println(localBaseDir.toString());


                Path localInDir = new Path(localBaseDir, "in");
                Path localWorkDir = new Path(localBaseDir, "work");
                Path localErrorDir = new Path(localBaseDir, "error");
                Path localCompleteDir = new Path(localBaseDir, "completed");

                localfs.delete(localBaseDir, true);

                localfs.mkdirs(localInDir);

                Path localInFile = new Path(localInDir, "test-file");
                TestFile inFile = new TestFile(localfs, localInFile);


                Path hdfsBaseDir = hdfs.makeQualified(new Path("/tmp", "test-slurper"));
                Path hdfsDestDir = new Path(hdfsBaseDir, "out");
                Path hdfsStageDir = new Path(hdfsBaseDir, "stage");

                System.out.println(hdfsBaseDir.toString());


                FileSystemManager fsm = new FileSystemManager(conf, localInDir, localWorkDir, localCompleteDir,
                        localErrorDir, hdfsDestDir, hdfsStageDir, false);

                WorkerThread wt = new WorkerThread(
                        conf,
                        true, // verify
                        false, // create done file
                        null, // script file
                        null, // work script
                        null, // codec
                        false, // create LZOP index
                        fsm,
                        1, // thread index
                        false, // poll
                        TimeUnit.MILLISECONDS, // poll wait time unit
                        0 // poll period
                );

                wt.run();

                Path expectedDestinationFile = new Path(hdfsDestDir, localInFile.getName());

                assertTrue("File doesn't exist '" + expectedDestinationFile.toUri() + "'", hdfs.exists(expectedDestinationFile));

                assertEquals(inFile.getCRC32(), hdfsFileCRC32(hdfs, null, expectedDestinationFile));

            }
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }


}
