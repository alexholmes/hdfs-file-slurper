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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Slurper {
    private static Log log = LogFactory.getLog(Slurper.class);

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
    private boolean doneFile;
    private boolean verify;
    private int numThreads = 1;
    private long pollSleepPeriodMillis = 1000;
    private boolean daemon;
    FileSystem srcFs;
    FileSystem destFs;
    Options options = new Options();
    Configuration config = new Configuration();

    public static final String ARGS_SOURCE_DIR = "src-dir";
    public static final String ARGS_WORK_DIR = "work-dir";
    public static final String ARGS_DEST_DIR = "dest-dir";
    public static final String ARGS_DEST_STAGING_DIR = "dest-staging-dir";
    public static final String ARGS_COMPRESS = "compress";
    public static final String ARGS_COMPRESS_LZO_CREATE_INDEX = "create-lzo-index";
    public static final String ARGS_DAEMON = "daemon";
    public static final String ARGS_DATASOURCE_NAME = "datasource-name";
    public static final String ARGS_DAEMON_NO_BACKGROUND = "daemon-no-bkgrnd";
    public static final String ARGS_CREATE_DONE_FILE = "create-done-file";
    public static final String ARGS_VERIFY = "verify";
    public static final String ARGS_REMOVE_AFTER_COPY = "remove-after-copy";
    public static final String ARGS_COMPLETE_DIR = "complete-dir";
    public static final String ARGS_ERROR_DIR = "error-dir";
    public static final String ARGS_WORKER_THREADS = "threads";
    public static final String ARGS_POLL_PERIOD_MILLIS = "poll-period-millis";
    public static final String ARGS_SCRIPT = "script";
    public static final String ARGS_WORK_SCRIPT = "work-script";

    private void printUsageAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Slurper", options, true);
        log.info("Exiting");
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

    private void setupLog4j(String datasourceName) throws IOException {
        String propFile = System.getProperty("slurper.log4j.properties");
        Properties p = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(propFile);
            p.load(is);
            p.put("log.datasource", datasourceName); // overwrite "log.dir"
            PropertyConfigurator.configure(p);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private void setupOptions() {

        String fullyQualifiedURIStory = "This must be a fully-qualified URI. " +
                " For example, for a local  /tmp directory, this would be file:/tmp.  " +
                "For a /tmp directory in HDFS, this would be hdfs://localhost:8020/tmp or hdfs:/tmp if you wanted to" +
                " use the NameNode host and port settings in your core-site.xml file.";

        // required arguments
        //
        options.addOption(createRequiredOption("d", ARGS_DATASOURCE_NAME, true, "The data source name.  This is used to log slurper activity to a unique log file.  "));
        options.addOption(createRequiredOption("s", ARGS_SOURCE_DIR, true, "Source directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("w", ARGS_WORK_DIR, true, "Work directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("e", ARGS_ERROR_DIR, true, "Error directory.  " + fullyQualifiedURIStory));
        options.addOption(createRequiredOption("g", ARGS_DEST_STAGING_DIR, true, "Staging directory.  Files are first copied into this directory, and after the copy has been completed and verified, they are moved into the destination directory.  " + fullyQualifiedURIStory));

        // optional arguments
        //
        options.addOption("a", ARGS_DAEMON, false, "Whether to run as a daemon (always up), or just process the existing files and exit.  This option will also 'nohup' the process");
        options.addOption("u", ARGS_DAEMON_NO_BACKGROUND, false, "Whether to run as a daemon (always up), or just process the existing files and exit.  This option is suitable for inittab respawn execution, where the Java process isn't launched in the background.");
        options.addOption("c", ARGS_COMPRESS, true, "The codec to use to compress the file as it is being written to the destination.  (Optional)");
        options.addOption("y", ARGS_COMPRESS_LZO_CREATE_INDEX, false, "If the compression codec is com.hadoop.compression.lzo.LzopCodec, "+
                " an index file will be created post transfer.  (Optional)");
        options.addOption("n", ARGS_CREATE_DONE_FILE, false, "Touch a file in the destination directory after the file " +
                "copy process has completed.  The done filename is the same as the destination file appended " +
                "with \".done\" (Optional)");
        options.addOption("v", ARGS_VERIFY, false, "Verify the file after it has been copied.  This is slow as it " +
                "involves reading the entire destination file after the copy has completed. (Optional)");
        options.addOption("p", ARGS_POLL_PERIOD_MILLIS, false, "The time threads wait in milliseconds between polling the file system for new files. (Optional)");
        options.addOption("x", ARGS_WORKER_THREADS, true, "The number of worker threads.  (Optional)");
        options.addOption("z", ARGS_WORK_SCRIPT, true, "A shell script which can be called to after the file is moved into the work directory but before it is copied to the destination." +
                "This gives users the chance to modify the contents of the file and change the filename prior to it being uploaded by the Slurper." +
                "An example of usage would be if files are dumped into the in folder and you need to uncompress them and also change the filename to include a timestamp." +
                "The standard input will contain a single line with the fully qualified URI of the source file in the work directory." +
                "The script must create a file in the word directory and return the fully-qualified URI of the new file in the work directory on standard output."
                );


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
            System.err.println("Could not parse command line args: " + e.getMessage());
            printUsageAndExit(options, 1);
            return;
        }

        srcDir = new Path(getRequiredOption(commandLine, ARGS_SOURCE_DIR));
        workDir = new Path(getRequiredOption(commandLine, ARGS_WORK_DIR));
        errorDir = new Path(getRequiredOption(commandLine, ARGS_ERROR_DIR));
        destStagingDir = new Path(getRequiredOption(commandLine, ARGS_DEST_STAGING_DIR));
        setupLog4j(getRequiredOption(commandLine, ARGS_DATASOURCE_NAME));

        String dir = commandLine.getOptionValue(ARGS_COMPLETE_DIR);
        if (dir != null) {
            completeDir = new Path(dir);
        }

        daemon = commandLine.hasOption(ARGS_DAEMON) || commandLine.hasOption(ARGS_DAEMON_NO_BACKGROUND);

        if (commandLine.hasOption(ARGS_POLL_PERIOD_MILLIS)) {
            pollSleepPeriodMillis = Integer.valueOf(commandLine.getOptionValue(ARGS_POLL_PERIOD_MILLIS));
        }

        if (commandLine.hasOption(ARGS_WORKER_THREADS)) {
            numThreads = Integer.valueOf(commandLine.getOptionValue(ARGS_WORKER_THREADS));
        }


        if (commandLine.hasOption(ARGS_COMPRESS)) {
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(Class.forName(commandLine.getOptionValue(ARGS_COMPRESS)), config);
            if (commandLine.hasOption(ARGS_COMPRESS)) {
                createLzopIndex = true;
            }
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

        validateSrcDir();

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

        if (commandLine.hasOption(ARGS_WORK_SCRIPT)) {
            workScript = commandLine.getOptionValue(ARGS_WORK_SCRIPT);
        }

        validateDirectories();
    }

    private void validateSrcDir() throws IOException {
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
        if (!compareFs(fs1, fs2)) {
            log.error("The two paths must exist on the same file system: " + p1 + "," + p2);
            printUsageAndExit(options, 3);
        }

        if (p1.equals(p2)) {
            log.error("The paths must be distinct: " + p1);
            printUsageAndExit(options, 3);
        }
    }

    private boolean compareFs(FileSystem fs1, FileSystem fs2) {
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

    private void testCreateSrcDir(Path p) throws IOException {
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

        if (destDir != null) {
            System.out.println("Comparing dest " + destDir + " and " + destStagingDir);
            validateSameFileSystem(destDir, destStagingDir);
        }

        testCreateSrcDir(workDir);
        testCreateSrcDir(errorDir);

        if (!remove) {
            validateSameFileSystem(srcDir, completeDir);
            testCreateSrcDir(completeDir);
        }
    }

    public static Option createRequiredOption(String opt, String longOpt, boolean hasArg, String description) {
        Option o = new Option(opt, longOpt, hasArg, description);
        o.setRequired(true);
        return o;
    }

    private void run() throws IOException, InterruptedException {

        FileSystemManager fileSystemManager = new FileSystemManager(config, srcDir, workDir, completeDir, errorDir,
                destDir, destStagingDir, remove);

        log.info("Moving any files in work directory to error directory");

        fileSystemManager.moveWorkFilesToError();

        final List<WorkerThread> workerThreads = new ArrayList<WorkerThread>();
        for (int i = 1; i <= numThreads; i++) {
            WorkerThread t = new WorkerThread(config, verify, doneFile, script, workScript, codec, createLzopIndex,
                    fileSystemManager, i, daemon, TimeUnit.MILLISECONDS, pollSleepPeriodMillis);
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
