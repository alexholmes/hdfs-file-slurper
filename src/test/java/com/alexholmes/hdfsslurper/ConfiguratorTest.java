package com.alexholmes.hdfsslurper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Before;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

public class ConfiguratorTest {

  Configuration conf = null;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @Test
  public void testValidateSameFileSystemHDFS() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost:8020");
    FileSystem fs1 = new Path("hdfs:/tmp/slurper/dest").getFileSystem(conf);
    FileSystem fs2 = new Path("hdfs:/tmp/slurper/stage").getFileSystem(conf);

    assertTrue(Configurator.compareFs(fs1, fs2));
  }

  @Test
  public void testValidateSameFileSystemHDFSDifferentPort() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost:8020");
    FileSystem fs1 = new Path("hdfs:/tmp/slurper/dest").getFileSystem(conf);
    conf.set("fs.defaultFS", "hdfs://localhost:8021");
    FileSystem fs2 = new Path("hdfs:/tmp/slurper/stage").getFileSystem(conf);

    assertFalse(Configurator.compareFs(fs1, fs2));
  }

  @Test
  public void testValidateSameFileSystemHDFSInplicitSchema() throws Exception {
    conf.set("fs.defaultFS", "hdfs://localhost:8020");
    FileSystem fs1 = new Path("/tmp/slurper/dest").getFileSystem(conf);
    FileSystem fs2 = new Path("/tmp/slurper/stage").getFileSystem(conf);

    assertTrue(Configurator.compareFs(fs1, fs2));
  }

  @Test
  public void testValidateSameFileSystemHDFSHA() throws Exception {
    conf.set("fs.defaultFS", "hdfs://mycluster");
    conf.set("dfs.nameservices", "mycluster");
    conf.set("dfs.ha.namenodes.mycluster", "A,B");
    conf.set("dfs.namenode.rpc-address.mycluster.A", "localhost:8020");
    conf.set("dfs.namenode.rpc-address.mycluster.B", "localhost:8021");
    conf.set("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    FileSystem fs1 = new Path("hdfs:/tmp/slurper/dest").getFileSystem(conf);
    FileSystem fs2 = new Path("hdfs:/tmp/slurper/stage").getFileSystem(conf);

    assertTrue(Configurator.compareFs(fs1, fs2));
  }

  @Test
  public void testValidateSameFileSystemLocalFs() throws Exception {
    FileSystem fs1 = new Path("file:/tmp/slurper/dest").getFileSystem(conf);
    FileSystem fs2 = new Path("file:/tmp/slurper/stage").getFileSystem(conf);

    assertTrue(Configurator.compareFs(fs1, fs2));
  }

  @Test
  public void testValidateSameFileSystemLocalFsImplicitSchema() throws Exception {
    FileSystem fs1 = new Path("/tmp/slurper/dest").getFileSystem(conf);
    FileSystem fs2 = new Path("/tmp/slurper/stage").getFileSystem(conf);

    assertTrue(Configurator.compareFs(fs1, fs2));
  }
}
