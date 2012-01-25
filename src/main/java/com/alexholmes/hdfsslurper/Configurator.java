/*
 * Copyright 2011 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alexholmes.hdfsslurper;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Configurator {

  public enum ConfigNames {
    DATASOURCE_NAME,
    SRC_DIR,
    WORK_DIR,
    COMPLETE_DIR,
    REMOVE_AFTER_COPY,
    ERROR_DIR,
    DEST_STAGING_DIR,
    DEST_DIR,
    COMPRESSION_CODEC,
    CREATE_LZO_INDEX,
    VERIFY,
    SCRIPT,
    THREADS,
    WORK_SCRIPT,
    POLL_MILLIS
  }

  private static Log log = LogFactory.getLog(Configurator.class);

  public static Config loadAndVerify(String path) throws IOException, MissingRequiredConfigException, ClassNotFoundException, MutuallyExclusiveConfigsExist, ConfigSettingException, FileSystemMkdirFailed, NoMutuallyExclusiveConfigsExist {
    Map<String, String> props = loadProperties(path);

    Config c = load(props);

    // make sure the mutually exclusive config names are flagged
    //
    verifyMutexOptions(c);

    // verify that all path's are in URI form, and that all the source and
    // all the destination paths are on the same file system (to make sure
    // that we can atomically move files around)
    //
    verifyPaths(c);
    
    return c;
  }

  public static Config load(Map<String, String> props) throws IOException, MissingRequiredConfigException, ClassNotFoundException {
    Config c = new Config();

    // set the Hadoop config
    //
    Configuration conf = new Configuration();
    c.setConfig(conf);

    // datasource name
    //
    c.setDatasource(getRequiredConfigValue(props, ConfigNames.DATASOURCE_NAME));

    // setup all the directories
    //
    c.setSrcDir(getRequiredConfigValueAsPath(props, ConfigNames.SRC_DIR))
        .setWorkDir(getRequiredConfigValueAsPath(props, ConfigNames.WORK_DIR))
        .setErrorDir(getRequiredConfigValueAsPath(props, ConfigNames.ERROR_DIR))
        .setCompleteDir(getConfigValueAsPath(props, ConfigNames.COMPLETE_DIR))
        .setDestDir(getConfigValueAsPath(props, ConfigNames.DEST_DIR))
        .setDestStagingDir(getRequiredConfigValueAsPath(props, ConfigNames.DEST_STAGING_DIR));

    // setup the file systems
    //
    c.setSrcFs(c.getSrcDir().getFileSystem(conf));
    if(c.getDestDir() != null) {
      c.setDestFs(c.getDestDir().getFileSystem(conf));
    }

    // compression
    //
    String compressionCodec = getConfigValue(props, ConfigNames.COMPRESSION_CODEC);
    if (compressionCodec != null) {
      c.setCodec((CompressionCodec)
          ReflectionUtils.newInstance(Class.forName(compressionCodec), conf));
    }
    c.setCreateLzopIndex(isOptionEnabled(props, ConfigNames.CREATE_LZO_INDEX));

    // scripts
    //
    c.setScript(getConfigValue(props, ConfigNames.SCRIPT));
    c.setWorkScript(getConfigValue(props, ConfigNames.WORK_SCRIPT));

    // additional options
    //
    c.setRemove(isOptionEnabled(props, ConfigNames.REMOVE_AFTER_COPY));
    c.setVerify(isOptionEnabled(props, ConfigNames.VERIFY));
    c.setNumThreads(getConfigValueAsInt(props, ConfigNames.THREADS, 1));
    c.setPollSleepPeriodMillis(getConfigValueAsInt(props, ConfigNames.POLL_MILLIS, 1000));

    return c;
  }

  private static void verifyMutexOptions(Config c) throws MutuallyExclusiveConfigsExist, NoMutuallyExclusiveConfigsExist {
    if(c.getDestDir() != null && c.getScript() != null) {
      throw new MutuallyExclusiveConfigsExist(ConfigNames.DEST_DIR, ConfigNames.SCRIPT);
    }

    if(c.getDestDir() == null && c.getScript() == null) {
      throw new NoMutuallyExclusiveConfigsExist(ConfigNames.DEST_DIR, ConfigNames.SCRIPT);
    }

    if(c.isRemove() && c.getCompleteDir() != null) {
      throw new MutuallyExclusiveConfigsExist(ConfigNames.REMOVE_AFTER_COPY, ConfigNames.COMPLETE_DIR);
    }

    if(!c.isRemove() && c.getCompleteDir() == null) {
      throw new NoMutuallyExclusiveConfigsExist(ConfigNames.REMOVE_AFTER_COPY, ConfigNames.COMPLETE_DIR);
    }
  }

  public static void verifyPaths(Config c) throws IOException, ConfigSettingException, FileSystemMkdirFailed {

    // validate source paths are complete URI's
    //
    checkScheme(c.getSrcDir(), ConfigNames.SRC_DIR);
    checkScheme(c.getWorkDir(), ConfigNames.WORK_DIR);
    checkScheme(c.getErrorDir(), ConfigNames.ERROR_DIR);
    if(c.getCompleteDir() != null) {
      checkScheme(c.getCompleteDir(), ConfigNames.COMPLETE_DIR);
    }

    // validate that the source directories are all on the same file system
    //
    validateSameFileSystem(c.getSrcDir(), c.getWorkDir(), c.getConfig());
    validateSameFileSystem(c.getSrcDir(), c.getErrorDir(), c.getConfig());
    if(c.getCompleteDir() != null) {
      validateSameFileSystem(c.getSrcDir(), c.getCompleteDir(), c.getConfig());
    }

    // create the source directories if they don't exist
    //
    testCreateDir(c.getSrcDir(), c.getConfig());
    testCreateDir(c.getWorkDir(), c.getConfig());
    testCreateDir(c.getErrorDir(), c.getConfig());
    if(c.getCompleteDir() != null) {
      testCreateDir(c.getCompleteDir(), c.getConfig());
    }

    // validate source paths are complete URI's
    //
    checkScheme(c.getDestStagingDir(), ConfigNames.DEST_STAGING_DIR);
    if (c.getDestDir() != null) {
      checkScheme(c.getDestDir(), ConfigNames.DEST_DIR);
    }

    // validate that the destination directories are all on the same file system
    //
    if (c.getDestDir() != null) {
      validateSameFileSystem(c.getDestDir(), c.getDestStagingDir(), c.getConfig());
    }

    // create the destination directories if they don't exist
    //
    testCreateDir(c.getDestStagingDir(), c.getConfig());
    if (c.getDestDir() != null) {
      testCreateDir(c.getDestDir(), c.getConfig());
    }
  }

  public static void checkScheme(Path p, ConfigNames config) throws ConfigSettingException {
    if (StringUtils.isBlank(p.toUri().getScheme())) {
      throw new ConfigSettingException("The " + config.name() + " scheme cannot be null." +
          " An example of a valid scheme is 'hdfs://localhost:8020/tmp' or 'file:/tmp'");
    }
  }

  public static void validateSameFileSystem(Path p1, Path p2, Configuration config) throws IOException, ConfigSettingException {
    FileSystem fs1 = p1.getFileSystem(config);
    FileSystem fs2 = p2.getFileSystem(config);
    if (!compareFs(fs1, fs2)) {
      throw new ConfigSettingException("The two paths must exist on the same file system: " + p1 + "," + p2);
    }

    if (p1.equals(p2)) {
      throw new ConfigSettingException("The paths must be distinct: " + p1);
    }
  }

  public static boolean compareFs(FileSystem fs1, FileSystem fs2) {
    URI uri1 = fs1.getUri();
    URI uri2 = fs2.getUri();
    if (uri1.getScheme() == null) {
      return false;
    }
    if (!uri1.getScheme().equals(uri2.getScheme())) {
      return false;
    }
    String srcHost = uri1.getHost();
    String dstHost = uri2.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost == null && dstHost != null) {
      return false;
    } else if (srcHost != null) {
      return false;
    }
    //check for ports
    return uri1.getPort() == uri2.getPort();
  }

  public static void testCreateDir(Path p, Configuration conf) throws IOException, ConfigSettingException, FileSystemMkdirFailed {
    FileSystem fs = p.getFileSystem(conf);
    if (fs.exists(p) && !fs.getFileStatus(p).isDir()) {
      throw new ConfigSettingException("Directory appears to be a file: '" + p + "'");
    }

    if (!fs.exists(p)) {
      log.info("Attempting creation of directory: " + p);
      if (!fs.mkdirs(p)) {
        throw new FileSystemMkdirFailed("Failed to create directory: '" + p + "'");
      }
    }
  }


  public static String getConfigValue(Map<String, String> props, ConfigNames key) {
    return props.get(key.name());
  }

  public static Path getConfigValueAsPath(Map<String, String> props, ConfigNames key) {
    String val = getConfigValue(props, key);
    if (val != null) {
      return new Path(val);
    }
    return null;
  }

  public static Path getRequiredConfigValueAsPath(Map<String, String> props, ConfigNames key) throws MissingRequiredConfigException {
    Path val = getConfigValueAsPath(props, key);
    if (val == null) {
      throw new MissingRequiredConfigException(key);
    }
    return val;
  }

  public static Integer getConfigValueAsInt(Map<String, String> props, ConfigNames key) {
    String val = getConfigValue(props, key);
    if (val != null) {
      return Integer.valueOf(val);
    }
    return null;
  }

  public static Integer getConfigValueAsInt(Map<String, String> props, ConfigNames key, Integer defaultValue) {
    String val = getConfigValue(props, key);
    if (val != null) {
      return Integer.valueOf(val);
    }
    return defaultValue;
  }

  public static String getRequiredConfigValue(Map<String, String> props, ConfigNames key) throws MissingRequiredConfigException {
    String val = getConfigValue(props, key);
    if (val == null) {
      throw new MissingRequiredConfigException(key);
    }
    return val;
  }

  public static int getRequiredConfigValueAsInt(Map<String, String> props, ConfigNames key) throws MissingRequiredConfigException {
    Integer val = getConfigValueAsInt(props, key);
    if (val == null) {
      throw new MissingRequiredConfigException(key);
    }
    return val;
  }

  public static boolean isOptionEnabled(Map<String, String> props, ConfigNames key) {
    String val = getConfigValue(props, key);
    return val != null && "true".equals(val.toLowerCase());
  }

  public static class MissingRequiredConfigException extends Throwable {
    private final ConfigNames key;

    public MissingRequiredConfigException(ConfigNames key) {
      this.key = key;
    }

    public ConfigNames getKey() {
      return key;
    }
  }

  public static class MutuallyExclusiveConfigsExist extends Throwable {
    private final ConfigNames key1;
    private final ConfigNames key2;

    public MutuallyExclusiveConfigsExist(ConfigNames key1, ConfigNames key2) {
      this.key1 = key1;
      this.key2 = key2;
    }

    public ConfigNames getKey1() {
      return key1;
    }

    public ConfigNames getKey2() {
      return key2;
    }
  }

  public static class NoMutuallyExclusiveConfigsExist  extends Throwable {
    private final ConfigNames key1;
    private final ConfigNames key2;

    public NoMutuallyExclusiveConfigsExist(ConfigNames key1, ConfigNames key2) {
      this.key1 = key1;
      this.key2 = key2;
    }

    public ConfigNames getKey1() {
      return key1;
    }

    public ConfigNames getKey2() {
      return key2;
    }
  }

  public static Map<String, String> loadProperties(String path) throws IOException {
    InputStream is = new FileInputStream(path);
    Properties properties = new Properties();
    properties.load(is);
    is.close();

    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry entry : properties.entrySet()) {
      props.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return props;
  }

  public static class ConfigSettingException extends Throwable {
    public ConfigSettingException(String s) {
      super(s);
    }
  }

  public static class FileSystemMkdirFailed extends Throwable {
    public FileSystemMkdirFailed(String s) {
      super(s);
    }
  }
}
