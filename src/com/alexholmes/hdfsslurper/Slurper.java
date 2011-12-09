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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Slurper {
    private static Log log = LogFactory.getLog(Slurper.class);

    private CompressionCodec codec;
    private Path srcDir;
    private Path workDir;
    private Path completeDir;
    private Path errorDir;
    private Path destDir;
    private String script;
    private boolean remove;
    private boolean doneFile;
    private boolean verify;
    private int numThreads;
    private long pollSleepPeriodMillis = 1000;
    private boolean daemon;
    FileSystem srcFs;
    FileSystem destFs;
    Options options = new Options();
    Configuration config = new Configuration();

    public static final String ARGS_SOURCE_DIR = "src-dir";
    public static final String ARGS_WORK_DIR = "work-dir";
    public static final String ARGS_DEST_DIR = "dest-dir";
    public static final String ARGS_COMPRESS = "compress";
    public static final String ARGS_DAEMON = "daemon";
    public static final String ARGS_CREATE_DONE_FILE = "create-done-file";
    public static final String ARGS_VERIFY = "verify";
    public static final String ARGS_REMOVE_AFTER_COPY = "remove-after-copy";
    public static final String ARGS_COMPLETE_DIR = "complete-dir";
    public static final String ARGS_ERROR_DIR = "error-dir";
    public static final String ARGS_WORKER_THREADS = "threads";
    public static final String ARGS_POLL_PERIOD_MILLIS = "poll-period-millis";
    public static final String ARGS_SCRIPT = "script";

    private void printUsageAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Slurper", options, true);
        System.exit(exitCode);
    }

    public void configure(String... args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

        // load the configuration into our members
        //
        setupOptions();

        // load and validate the configuration
        //
        loadAndValidateOptions(args);
    }

    private void setupOptions() {

        String fullyQualifiedURIStory = "This must be a fully-qualified URI. " +
                " For example, for a local  /tmp directory, this would be file:/tmp.  " +
                "For a /tmp directory in HDFS, this would be hdfs://localhost:8020/tmp or hdfs:/tmp if you wanted to" +
                " use the NameNode host and port settings in your core-site.xml file.";

        // required arguments
        //
        options.addOption(createRequiredOption("s", ARGS_SOURCE_DIR, true, "Source directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("w", ARGS_WORK_DIR, true, "Work directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("e", ARGS_ERROR_DIR, true, "Error directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("x", ARGS_WORKER_THREADS, true, "The number of worker threads"));

        // optional arguments
        //
        options.addOption("a", ARGS_DAEMON, false, "Whether to run as a daemon (always up), or just process the existing files and exit.");
        options.addOption("c", ARGS_COMPRESS, true, "The compression codec class (Optional)");
        options.addOption("n", ARGS_CREATE_DONE_FILE, false, "Touch a file in the destination directory after the file " +
                "copy process has completed.  The done filename is the same as the destination file appended " +
                "with \".done\" (Optional)");
        options.addOption("v", ARGS_VERIFY, false, "Verify the file after it has been copied.  This is slow as it " +
                "involves reading the entire destination file after the copy has completed. (Optional)");
        options.addOption("p", ARGS_POLL_PERIOD_MILLIS, false, "The time threads wait in milliseconds between polling the file system for new files. (Optional)");

        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("t", ARGS_DEST_DIR, true, "Destination directory where files should be copied. " +
                "Either this or the \"" + ARGS_SCRIPT + "\" option must be set. " + fullyQualifiedURIStory);
        options.addOption("i", ARGS_SCRIPT, true, "A shell script which can be called to determine the destination directory." +
                "The standard input will contain a single line with the fully qualified URI of the source file, and the script must put the destination " +
                " full path on standard output. " + fullyQualifiedURIStory + "  Either this or the \"hdfsdif\" option must be set.");


        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("r", ARGS_REMOVE_AFTER_COPY, false, "Remove the source file after a successful copy.  Either this or the \"completedir\" option must be set.");
        options.addOption("o", ARGS_COMPLETE_DIR, true, "Completion directory where file is moved after successful copy.  Must be in the same filesystem as the source file.  Either this or the \"remove\" option must be set.");
    }

    public void checkScheme(Path p, String typeOfPath) {
        if (StringUtils.isBlank(p.toUri().getScheme())) {
            log.error("The " + typeOfPath + " scheme cannot be null.  An example of a valid scheme is 'hdfs://localhost:8020/tmp' or 'file:/tmp'");
            printUsageAndExit(options, 50);
        }
    }

    private String getRequiredOption(CommandLine commandLine, String option) {
        if (!commandLine.hasOption(option)) {
            log.error(option + " is a required option");
            printUsageAndExit(options, 2);
        }
        return commandLine.getOptionValue(option);
    }

    private void loadAndValidateOptions(String... args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        // parse command line parameters
        CommandLine commandLine;
        try {
            commandLine = new PosixParser().parse(options, args);
        } catch (ParseException e) {
            log.error("Could not parse command line args: " + e.getMessage());
            printUsageAndExit(options, 1);
            return;
        }

        srcDir = new Path(getRequiredOption(commandLine, ARGS_SOURCE_DIR));
        workDir = new Path(getRequiredOption(commandLine, ARGS_WORK_DIR));
        errorDir = new Path(getRequiredOption(commandLine, ARGS_ERROR_DIR));

        String dir = commandLine.getOptionValue(ARGS_COMPLETE_DIR);
        if (dir != null) {
            completeDir = new Path(dir);
        }

        daemon = commandLine.hasOption(ARGS_DAEMON);

        if (commandLine.hasOption(ARGS_POLL_PERIOD_MILLIS)) {
            pollSleepPeriodMillis = Integer.valueOf(commandLine.getOptionValue(ARGS_POLL_PERIOD_MILLIS));
        }

        if (!commandLine.hasOption(ARGS_WORKER_THREADS)) {
            log.error(ARGS_WORKER_THREADS + " must be set");
            printUsageAndExit(options, 4);
        }
        numThreads = Integer.valueOf(commandLine.getOptionValue(ARGS_WORKER_THREADS));


        if (commandLine.hasOption(ARGS_COMPRESS)) {
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(Class.forName(commandLine.getOptionValue(ARGS_COMPRESS)), config);
        }
        remove = commandLine.hasOption(ARGS_REMOVE_AFTER_COPY);
        doneFile = commandLine.hasOption(ARGS_CREATE_DONE_FILE);
        verify = commandLine.hasOption(ARGS_VERIFY);

        // make sure one of these has been set
        //
        if (!commandLine.hasOption(ARGS_REMOVE_AFTER_COPY) && !commandLine.hasOption(ARGS_COMPLETE_DIR)) {
            log.error("One of \"--remove\" or \"--complete-dir\" must be set");
            printUsageAndExit(options, 2);
        }

        validateLocalSrcDir();
        validateDirectories();

        // make sure one of these has been set
        //
        if (!commandLine.hasOption(ARGS_DEST_DIR) && !commandLine.hasOption(ARGS_SCRIPT)) {
            log.error("One of \"--dest-dir\" or \"--script\" must be set");
            printUsageAndExit(options, 4);
        }

        // make sure ONLY one of these has been set
        //
        if (commandLine.hasOption(ARGS_DEST_DIR) && commandLine.hasOption(ARGS_SCRIPT)) {
            log.error("Only one of \"--dest-dir\" or \"--script\" can be set");
            printUsageAndExit(options, 5);
        }


        if (commandLine.hasOption(ARGS_DEST_DIR)) {
            destDir = new Path(commandLine.getOptionValue(ARGS_DEST_DIR));
            checkScheme(destDir, "destination directory");
            destFs = FileSystem.get(destDir.toUri(), new Configuration());
        } else {
            script = commandLine.getOptionValue(ARGS_SCRIPT);
        }

    }

    private void validateLocalSrcDir() throws IOException {
        checkScheme(srcDir, "source directory");
        srcFs = FileSystem.get(srcDir.toUri(), new Configuration());
        if (!srcFs.exists(srcDir) || !srcFs.getFileStatus(srcDir).isDir()) {
            log.error("Source directory must exist: " + srcDir);
            printUsageAndExit(options, 20);
        }
    }

    private void validateSameFileSystem(Path p1, Path p2) throws IOException {
        FileSystem fs1 = p1.getFileSystem(config);
        FileSystem fs2 = p2.getFileSystem(config);
        URI u1 = fs1.getUri();
        URI u2 = fs2.getUri();
        if (!u1.equals(u2)) {
            log.error("The two paths must exist on the same file system: " + p1 + "," + p2);
            printUsageAndExit(options, 3);
        }

        if (p1.equals(p2)) {
            log.error("The paths must be distinct: " + p1);
            printUsageAndExit(options, 3);
        }
    }

    private void testCreateDir(Path p) throws IOException {
        if (srcFs.exists(p) && !srcFs.getFileStatus(p).isDir()) {
            log.error("Directory appears to be a file: " + p);
            printUsageAndExit(options, 40);
        }

        if (!srcFs.exists(p)) {
            log.info("Attempting creation of directory: " + p);
            if (!srcFs.mkdirs(p)) {
                log.error("Failed to create directory: " + p);
                printUsageAndExit(options, 41);
            }
        }
    }

    private void validateDirectories() throws IOException {
        validateSameFileSystem(srcDir, workDir);
        validateSameFileSystem(srcDir, errorDir);

        testCreateDir(workDir);
        testCreateDir(errorDir);

        if (!remove) {
            validateSameFileSystem(srcDir, completeDir);
            testCreateDir(completeDir);
        }
    }

    public static Option createRequiredOption(String opt, String longOpt, boolean hasArg, String description) {
        Option o = new Option(opt, longOpt, hasArg, description);
        o.setRequired(true);
        return o;
    }

    private void run() throws IOException, InterruptedException {

        FileSystemManager fileSystemManager = new FileSystemManager(config, srcDir, workDir, completeDir, errorDir,
                destDir, remove);

        log.info("Moving any files in work directory to error directory");

        fileSystemManager.moveWorkFilesToError();

        final List<WorkerThread> workerThreads = new ArrayList<WorkerThread>();
        for (int i = 1; i <= numThreads; i++) {
            WorkerThread t = new WorkerThread(config, verify, doneFile, script, codec, fileSystemManager,
                    i, daemon, TimeUnit.MILLISECONDS, pollSleepPeriodMillis);
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

    public static void main(String... args) {
        try {
            Slurper slurper = new Slurper();
            slurper.configure(args);
            slurper.run();
        } catch (Throwable t) {
            log.error("Caught exception in main()", t);
            System.exit(1000);
        }
    }
}
