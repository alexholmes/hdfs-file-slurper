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

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Slurper extends Configured implements Tool {
  private static Log log = LogFactory.getLog(Slurper.class);
  public static final String ARGS_CONFIG_FILE = "config-file";
  public static final String ARGS_LOG4J_FILE = "log4j-file";

  private Config config;

  private void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Slurper", options, true);
    log.info("Exiting");
    System.exit(exitCode);
  }

  private void printErrorAndExit(String s, int exitCode) {
    log.error(s);
    System.err.println(s);
    log.info("Exiting");
    System.exit(exitCode);
  }

  public void configure(String... args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

    Options options = new Options();
    options.addOption("o", ARGS_CONFIG_FILE, true, "The configuration file (required). ");
    options.addOption("o", ARGS_LOG4J_FILE, true, "The log4j file (required). ");

    CommandLine commandLine;
    try {
      commandLine = new PosixParser().parse(options, args, false);
    } catch (ParseException e) {
      log.error("Could not parse command line args: " + e.getMessage());
      System.err.println("Could not parse command line args: " + e.getMessage());
      printUsageAndExit(options, 1);
      return;
    }

    String path = commandLine.getOptionValue(ARGS_CONFIG_FILE);
    if (path == null) {
      System.err.println("Missing required argument " + ARGS_CONFIG_FILE);
      printUsageAndExit(options, 2);
    }

    String log4jPath = commandLine.getOptionValue(ARGS_LOG4J_FILE);
    if (log4jPath == null) {
      System.err.println("Missing required argument " + ARGS_LOG4J_FILE);
      printUsageAndExit(options, 3);
    }

    System.out.println("Conf = " + getConf());

    try {
      config = Configurator.loadAndVerify(getConf(), path);
    } catch (Configurator.MissingRequiredConfigException e) {
      printErrorAndExit("Missing required option in config file: " + e.getKey(), 10);
    } catch (Configurator.MutuallyExclusiveConfigsExist e2) {
      printErrorAndExit("Mutually exclusive options are both set (only one should be set): " + e2.getKey1() +
          ", " + e2.getKey2(), 11);
    } catch (Configurator.ConfigSettingException e) {
      printErrorAndExit(e.getMessage(), 12);
    } catch (Configurator.FileSystemMkdirFailed e3) {
      printErrorAndExit(e3.getMessage(), 13);
    } catch (Configurator.NoMutuallyExclusiveConfigsExist e4) {
      printErrorAndExit("One of these mutually exclusive options must be set: " + e4.getKey1() +
          ", " + e4.getKey2(), 14);
    }

    setupLog4j(log4jPath, config.getDatasource());
  }

  private void setupLog4j(String log4jPath, String datasourceName) throws IOException {
    Properties p = new Properties();
    InputStream is = null;
    try {
      is = new FileInputStream(log4jPath);
      p.load(is);
      p.put("log.datasource", datasourceName); // overwrite "log.dir"
      PropertyConfigurator.configure(p);
    } finally {
      IOUtils.closeQuietly(is);
    }
  }

  private void run() throws IOException, InterruptedException {

    FileSystemManager fileSystemManager = new FileSystemManager(config);

    log.info("Moving any files in work directory to error directory");

    fileSystemManager.moveWorkFilesToError();

    final List<WorkerThread> workerThreads = new ArrayList<WorkerThread>();
    for (int i = 1; i <= config.getNumThreads(); i++) {
      WorkerThread t = new WorkerThread(config, fileSystemManager, TimeUnit.MILLISECONDS, i);
      t.start();
      workerThreads.add(t);
    }

    final AtomicBoolean programmaticShutdown = new AtomicBoolean(false);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (programmaticShutdown.get()) {
            log.info("JVM shutting down");
          } else {
            log.info("External process signalled JVM shutdown, shutting down threads.");
            log.info("This may take a few minutes until we let the threads complete ");
            log.info("the current file being copied.");
            for (WorkerThread workerThread : workerThreads) {
              workerThread.shutdown();
            }
            log.info("Threads dead");
          }
        } catch (Throwable t) {
          log.error("Hit snag in shutdown hook", t);
        }

      }
    });

    log.info("Running");

    for (WorkerThread workerThread : workerThreads) {
      workerThread.join();
    }
    programmaticShutdown.set(true);
  }

  /**
   * Main entry point.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    Slurper slurper = new Slurper();
    int res = ToolRunner.run(new Configuration(), slurper, args);
    System.exit(res);
  }

  /**
   * The MapReduce driver - setup and launch the job.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {
    try {
      configure(args);
      run();
      return 0;
    } catch (Throwable t) {
      log.error("Caught exception in main()", t);
      t.printStackTrace();
      return 1000;
    }
  }
}
