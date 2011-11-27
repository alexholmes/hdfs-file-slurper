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
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class Slurper {
    private static Log log = LogFactory.getLog(Slurper.class);

    private CompressionCodec codec;
    private Path srcDir;
    private Path completeDir;
    private Path destDir;
    private String script;
    private boolean remove;
    private boolean dryRun;
    private boolean doneFile;
    private boolean verify;
    FileSystem srcFs;
    FileSystem destFs;
    Options options = new Options();
    Configuration config = new Configuration();
    
    public static final String ARGS_SOURCE_DIR = "src-dir";
    public static final String ARGS_DEST_DIR = "dest-dir";
    public static final String ARGS_COMPRESS = "compress";
    public static final String ARGS_DRY_RUN = "dry-run";
    public static final String ARGS_CREATE_DONE_FILE = "create-done-file";
    public static final String ARGS_VERIFY = "verify";
    public static final String ARGS_REMOVE_AFTER_COPY = "remove-after-copy";
    public static final String ARGS_COMPLETE_DIR = "complete-dir";

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

        String fullyQualifiedURIStory =  "This must be a fully-qualified URI. " +
                " For example, for a local  /tmp directory, this would be file:/tmp.  " +
                "For a /tmp directory in HDFS, this would be hdfs://localhost:8020/tmp or hdfs:/tmp if you wanted to" +
                " use the NameNode host and port settings in your core-site.xml file.";

        // required arguments
        //
        options.addOption(createRequiredOption("s", ARGS_SOURCE_DIR, true, "Source directory.  " + fullyQualifiedURIStory));

        // optional arguments
        //
        options.addOption("c", ARGS_COMPRESS, true, "The compression codec class (Optional)");
        options.addOption("d", ARGS_DRY_RUN, false, "Perform a dry run - do not actually copy the files (Optional)");
        options.addOption("n", ARGS_CREATE_DONE_FILE, false, "Touch a file in the destination directory after the file " +
                "copy process has completed.  The done filename is the same as the destination file appended " +
                "with \".done\" (Optional)");
        options.addOption("v", ARGS_VERIFY, false, "Verify the file after it has been copied.  This is slow as it " +
                "involves reading the entire destination file after the copy has completed. (Optional)");

        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("t", ARGS_DEST_DIR, true, "Destination directory where files should be copied. " +
                "Either this or the \"script\" option must be set. " + fullyQualifiedURIStory);
        options.addOption("i", "script", true, "A shell script which can be called to determine the destination directory." +
                "The standard input will contain a single line with the fully qualified URI of the source file, and the script must put the destination " +
                " full path on standard output. " + fullyQualifiedURIStory + "  Either this or the \"hdfsdif\" option must be set.");


        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("r", ARGS_REMOVE_AFTER_COPY, false, "Remove the source file after a successful copy.  Either this or the \"completedir\" option must be set.");
        options.addOption("o", ARGS_COMPLETE_DIR, true, "Completion directory where file is moved after successful copy.  Must be in the same filesystem as the source file.  Either this or the \"remove\" option must be set.");
    }

    public void checkScheme(Path p, String typeOfPath) {
        if(StringUtils.isBlank(p.toUri().getScheme())) {
            log.error("The " + typeOfPath + " scheme cannot be null.  An example of a valid scheme is 'hdfs://localhost:8020/tmp' or 'file:/tmp'");
            printUsageAndExit(options, 50);
        }
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

        srcDir = new Path(commandLine.getOptionValue(ARGS_SOURCE_DIR));
        String dir = commandLine.getOptionValue(ARGS_COMPLETE_DIR);
        if(dir != null) {
            completeDir = new Path(dir);
        }

        if (commandLine.hasOption(ARGS_COMPRESS)) {
            codec = (CompressionCodec)
                    ReflectionUtils.newInstance(Class.forName(commandLine.getOptionValue(ARGS_COMPRESS)), config);
        }
        remove = commandLine.hasOption(ARGS_REMOVE_AFTER_COPY);
        dryRun = commandLine.hasOption(ARGS_DRY_RUN);
        doneFile = commandLine.hasOption(ARGS_CREATE_DONE_FILE);
        verify = commandLine.hasOption(ARGS_VERIFY);

        if (dryRun) {
            log.info("Dry-run mode, no files will be copied");
        }

        // make sure one of these has been set
        //
        if (!commandLine.hasOption(ARGS_REMOVE_AFTER_COPY) && !commandLine.hasOption(ARGS_COMPLETE_DIR)) {
            log.error("One of \"--remove\" or \"--complete-dir\" must be set");
            printUsageAndExit(options, 2);
        }

        validateLocalSrcDir();
        validateLocalCompleteDir();

        // make sure one of these has been set
        //
        if (!commandLine.hasOption(ARGS_DEST_DIR) && !commandLine.hasOption("script")) {
            log.error("One of \"--dest-dir\" or \"--script\" must be set");
            printUsageAndExit(options, 4);
        }

        // make sure ONLY one of these has been set
        //
        if (commandLine.hasOption(ARGS_DEST_DIR) && commandLine.hasOption("script")) {
            log.error("Only one of \"--dest-dir\" or \"--script\" can be set");
            printUsageAndExit(options, 5);
        }


        if (commandLine.hasOption(ARGS_DEST_DIR)) {
            destDir = new Path(commandLine.getOptionValue(ARGS_DEST_DIR));
            checkScheme(destDir, "destination directory");
            destFs = FileSystem.get(destDir.toUri(), new Configuration());
        } else {
            script = commandLine.getOptionValue("script");
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

    private void validateLocalCompleteDir() throws IOException {
        if (!remove) {
            checkScheme(completeDir, "complete directory");

            // make sure the scheme is the same for the source and complete dir, otherwise the move
            // won't be atomic
            //
            if(!srcDir.toUri().getScheme().equals(completeDir.toUri().getScheme())) {
                log.error("The source and completed directory schemes must match.");
                printUsageAndExit(options, 3);
            }

            if (srcFs.exists(completeDir) && !srcFs.getFileStatus(completeDir).isDir()) {
                log.error("Completed directory appears to be a file: " + completeDir);
                printUsageAndExit(options, 40);
            }

            if (!srcFs.exists(completeDir)) {
                log.info("Attempting creation of complete directory: " + completeDir);
                if (!srcFs.mkdirs(completeDir)) {
                    log.error("Failed to create complete directory: " + completeDir);
                    printUsageAndExit(options, 41);
                }
            }

            if (srcDir.toUri().equals(completeDir.toUri())) {
                log.error("Source and complete directories can't be the same!");
                printUsageAndExit(options, 43);
            }

        }
    }

    public static Option createRequiredOption(String opt, String longOpt, boolean hasArg, String description) {
        Option o = new Option(opt, longOpt, hasArg, description);
        o.setRequired(true);
        return o;
    }

    private void run() throws IOException {
        // read all the files in the local directory and process them serially
        //
        int successCopy = 0;
        int errorCopy = 0;
        for (FileStatus fs : srcFs.listStatus(srcDir)) {
            if (!fs.isDir()) {
                if(fs.getPath().getName().startsWith(".")) {
                    log.info("Ignoring hidden file '" + fs.getPath() + "'");
                    continue;
                }
                try {
                    process(fs);
                    successCopy++;
                } catch (IOException e) {
                    errorCopy++;
                    log.error("Hit snag attempting to copy file '" + fs.getPath() + "'", e);
                }
            }
        }
        if (!dryRun) {
            log.info("Completed with " + successCopy + " successful copies, and " + errorCopy + " errors");
        }
    }

    private void process(FileStatus srcFileStatus) throws IOException {

        Path srcFile = srcFileStatus.getPath();

        // get the target HDFS file
        //
        Path destFile = getHdfsTargetPath(srcFileStatus);

        log.info("Copying source file '" + srcFile + "' to destination '" + destFile + "'");

        if (dryRun) {
            return;
        }

        // if the directory of the target HDFS file doesn't exist, attempt to create it
        //
        Path destParentDir = destFile.getParent();
        if (!destFs.exists(destParentDir)) {
            log.info("Attempting creation of HDFS target directory: " + destParentDir);
            if (!destFs.mkdirs(destParentDir)) {
                throw new IOException("Failed to create target directory in HDFS: " + destParentDir);
            }
        }

        // copy the file
        //
        InputStream is = null;
        OutputStream os = null;
        CRC32 crc = new CRC32();
        try {
            is = new BufferedInputStream(srcFs.open(srcFile));
            if(verify) {
                is = new CheckedInputStream(is, crc);
            }
            os = destFs.create(destFile);

            if (codec != null) {
                os = codec.createOutputStream(os);
            }

            IOUtils.copyBytes(is, os, 4096, true);
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(os);
        }

        long srcFileSize = srcFs.getFileStatus(srcFile).getLen();
        long destFileSize = destFs.getFileStatus(destFile).getLen();
        if (codec == null && srcFileSize != destFileSize) {
            throw new IOException("File sizes don't match, source = " + srcFileSize + ", dest = " + destFileSize);
        }

        log.info("Local file size = " + srcFileSize + ", HDFS file size = " + destFileSize);

        if (verify) {
            verify(destFile, crc.getValue());
        }

        // remove or move the file away from the source directory
        //
        if (remove) {
            if (!srcFs.delete(srcFile, false)) {
                log.warn("Failed to delete file '" + srcFile + "'");
            }
        } else {
            Path completeFile = new Path(completeDir, srcFile.getName());
            if (!srcFs.rename(srcFile, completeFile)) {
                log.warn("Failed to move file from '" + srcFile + "' to '" + completeFile + "'");
            } else {
                log.info("Moved source file to " + completeFile);
            }
        }

        if (doneFile) {
            Path doneFile = new Path(destFile.getParent(), destFile.getName() + ".done");
            log.info("Touching done file " + doneFile);
            touch(doneFile);
        }
    }

    private void verify(Path hdfs, long localFileCRC) throws IOException {
        log.info("Verifying files");
        long hdfsCRC = hdfsFileCRC32(hdfs);

        if(localFileCRC != hdfsCRC) {
            throw new IOException("CRC's don't match, local file is " + localFileCRC + " HDFS file is " + hdfsCRC);
        }
        log.info("CRC's match (" + localFileCRC + ")");
    }

    private long hdfsFileCRC32(Path path) throws IOException {
        InputStream in = null;
        CRC32 crc = new CRC32();
        try {
            InputStream is = new BufferedInputStream(path.getFileSystem(config).open(path));
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

    private void touch(Path p) throws IOException {
        destFs.create(p).close();
    }

    private Path getHdfsTargetPath(FileStatus srcFile) throws IOException {
        if (destDir != null) {
            if (codec != null) {
                return new Path(destDir, srcFile.getPath().getName() + codec.getDefaultExtension());
            } else {
                return new Path(destDir, srcFile.getPath().getName());
            }
        } else {
            return getDestPathFromScript(srcFile);
        }
    }

    private Path getDestPathFromScript(FileStatus srcFile) throws IOException {
        Path p = new Path(ScriptExecutor.getDestFileFromScript(script, srcFile, 60, TimeUnit.SECONDS));
        if(p.toUri().getScheme() == null) {
            throw new IOException("Destination path from script must be a URI with a scheme: '" + p + "'");
        }
        destFs = FileSystem.get(p.toUri(), config);
        return p;
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
