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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

public class Config {

  private String datasource;
  private CompressionCodec codec;
  private boolean createLzopIndex;
  private Path srcDir;
  private Path workDir;
  private Path completeDir;
  private Path errorDir;
  private Path destDir;
  private Path destStagingDir;
  private String script;
  private String workScript;
  private boolean remove;
  private boolean verify;
  private int numThreads;
  private long pollSleepPeriodMillis;
  FileSystem srcFs;
  FileSystem destFs;
  Configuration config;


  public String getDatasource() {
    return datasource;
  }

  public Config setDatasource(String datasource) {
    this.datasource = datasource;
    return this;
  }

  public CompressionCodec getCodec() {
    return codec;
  }

  public Config setCodec(CompressionCodec codec) {
    this.codec = codec;
    return this;
  }

  public boolean isCreateLzopIndex() {
    return createLzopIndex;
  }

  public Config setCreateLzopIndex(boolean createLzopIndex) {
    this.createLzopIndex = createLzopIndex;
    return this;
  }

  public Path getSrcDir() {
    return srcDir;
  }

  public Config setSrcDir(Path srcDir) {
    this.srcDir = srcDir;
    return this;
  }

  public Path getWorkDir() {
    return workDir;
  }

  public Config setWorkDir(Path workDir) {
    this.workDir = workDir;
    return this;
  }

  public Path getCompleteDir() {
    return completeDir;
  }

  public Config setCompleteDir(Path completeDir) {
    this.completeDir = completeDir;
    return this;
  }

  public Path getErrorDir() {
    return errorDir;
  }

  public Config setErrorDir(Path errorDir) {
    this.errorDir = errorDir;
    return this;
  }

  public Path getDestDir() {
    return destDir;
  }

  public Config setDestDir(Path destDir) {
    this.destDir = destDir;
    return this;
  }

  public Path getDestStagingDir() {
    return destStagingDir;
  }

  public Config setDestStagingDir(Path destStagingDir) {
    this.destStagingDir = destStagingDir;
    return this;
  }

  public String getScript() {
    return script;
  }

  public Config setScript(String script) {
    this.script = script;
    return this;
  }

  public String getWorkScript() {
    return workScript;
  }

  public Config setWorkScript(String workScript) {
    this.workScript = workScript;
    return this;
  }

  public boolean isRemove() {
    return remove;
  }

  public Config setRemove(boolean remove) {
    this.remove = remove;
    return this;
  }

  public boolean isVerify() {
    return verify;
  }

  public Config setVerify(boolean verify) {
    this.verify = verify;
    return this;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public Config setNumThreads(int numThreads) {
    this.numThreads = numThreads;
    return this;
  }

  public long getPollSleepPeriodMillis() {
    return pollSleepPeriodMillis;
  }

  public Config setPollSleepPeriodMillis(long pollSleepPeriodMillis) {
    this.pollSleepPeriodMillis = pollSleepPeriodMillis;
    return this;
  }

  public FileSystem getSrcFs() {
    return srcFs;
  }

  public Config setSrcFs(FileSystem srcFs) {
    this.srcFs = srcFs;
    return this;
  }

  public FileSystem getDestFs() {
    return destFs;
  }

  public Config setDestFs(FileSystem destFs) {
    this.destFs = destFs;
    return this;
  }

  public Configuration getConfig() {
    return config;
  }

  public Config setConfig(Configuration config) {
    this.config = config;
    return this;
  }
}
