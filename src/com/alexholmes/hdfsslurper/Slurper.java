package com.alexholmes.hdfsslurper;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class Slurper {
    private static Log log = LogFactory.getLog(Slurper.class);

    private CompressionCodec codec;
    private File localSourceDir;
    private File localCompleteDir;
    private Path hdfsDestDir;
    private String script;
    private boolean remove;
    private boolean dryRun;
    FileSystem hdfsFs;
    Options options = new Options();

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
        // required arguments
        //
        options.addOption(createRequiredOption("s", "sourcedir", true, "Local filesystem source directory for files to be copied into HDFS"));

        // optional arguments
        //
        options.addOption("c", "compress", true, "The compression codec class (Optional)");
        options.addOption("d", "dryrun", false, "Perform a dry run - do not actually copy the files into HDFS (Optional)");

        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("t", "hdfsdir", true, "HDFS target directory where files should be copied. " +
            "Either this or the \"script\" option must be set.");
        options.addOption("i", "script", true, "A shell script which can be called to determine the HDFS target directory." +
                "The standard input will contain a single line with the source file, and the script must put the HDFS " +
                "target full path on standard output. Either this or the \"hdfsdif\" option must be set.");


        // mutually exclusive arguments.  one of them must be defined
        //
        options.addOption("r", "remove", false, "Remove local file after successful copy into HDFS.  Either this or the \"completedir\" option must be set.");
        options.addOption("o", "completedir", true, "Local filesystem completion directory where file is moved after successful copy into HDFS.  Either this or the \"remove\" option must be set.");

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

        localSourceDir = new File(commandLine.getOptionValue("sourcedir"));
        localCompleteDir = new File(commandLine.getOptionValue("completedir"));
        if (commandLine.hasOption("compress")) {
            codec = (CompressionCodec) Class.forName(commandLine.getOptionValue("compress")).newInstance();
        }
        remove = commandLine.hasOption("remove");
        dryRun = commandLine.hasOption("dryrun");

        if(dryRun) {
            log.info("Dry-run mode, no files will be copied");
        }

        // make sure one of these has been set
        //
        if(!commandLine.hasOption("remove") && !commandLine.hasOption("completedir")) {
            log.error("One of \"remove\" or \"completedir\" must be set");
            printUsageAndExit(options, 4);
        }

        // make sure ONLY one of these has been set
        //
        if(commandLine.hasOption("remove") && commandLine.hasOption("completedir")) {
            log.error("Only one of \"remove\" or \"completedir\" can be set");
            printUsageAndExit(options, 5);
        }

        validateLocalSrcDir();
        validateLocalCompleteDir();

        // make sure one of these has been set
        //
        if(!commandLine.hasOption("hdfsdir") && !commandLine.hasOption("script")) {
            log.error("One of \"hdfsdir\" or \"script\" must be set");
            printUsageAndExit(options, 4);
        }

        // make sure ONLY one of these has been set
        //
        if(commandLine.hasOption("hdfsdir") && commandLine.hasOption("script")) {
            log.error("Only one of \"hdfsdir\" or \"script\" can be set");
            printUsageAndExit(options, 5);
        }


        if(commandLine.hasOption("hdfsdir")) {
            hdfsDestDir = new Path(commandLine.getOptionValue("hdfsdir"));
        } else {
            script = commandLine.getOptionValue("script");
        }

        hdfsFs = FileSystem.get(new Configuration());
    }

    private void validateLocalSrcDir() {
        if (!localSourceDir.exists() || !localSourceDir.isDirectory()) {
            log.error("Local filesystem source directory must exist: " + localSourceDir.getAbsolutePath());
            printUsageAndExit(options, 2);
        }

        if (!localSourceDir.canWrite()) {
            log.error("Must have permissions to write to local filesystem source directory : " + localSourceDir.getAbsolutePath());
            printUsageAndExit(options, 3);
        }
    }

    private void validateLocalCompleteDir() {
        if (!remove) {
            if (localCompleteDir.exists() && !localSourceDir.isDirectory()) {
                log.error("Local filesystem complete directory must be a directory: " + localCompleteDir.getAbsolutePath());
                printUsageAndExit(options, 6);
            }
            if (!localCompleteDir.exists()) {
                log.info("Attempting creation of local filesystem complete directory: " + localCompleteDir.getAbsolutePath());
                if (!localCompleteDir.mkdir()) {
                    log.error("Failed to create local filesystem complete directory: " + localCompleteDir.getAbsolutePath());
                    printUsageAndExit(options, 7);
                }
            }
            if (!localCompleteDir.canWrite()) {
                log.error("Must be able to write to local filesystem complete directory: " + localCompleteDir.getAbsolutePath());
                printUsageAndExit(options, 8);
            }
            if (localSourceDir.equals(localCompleteDir)) {
                log.error("Local source and complete directories can't be the same!");
                printUsageAndExit(options, 9);
            }
        }
    }

    public static Option createRequiredOption(String opt, String longOpt, boolean hasArg, String description) {
        Option o = new Option(opt, longOpt, hasArg, description);
        o.setRequired(true);
        return o;
    }

    private void run() {
        // read all the files in the local directory and process them serially
        //
        int successCopy = 0;
        int errorCopy = 0;
        for (File file : localSourceDir.listFiles()) {
            if(file.isFile()) {
                try {
                    process(file);
                    successCopy++;
                } catch (IOException e) {
                    errorCopy++;
                    log.error("Hit snag attempting to copy file '" + file.getAbsolutePath() + "'", e);
                }
            }
        }
        if(!dryRun) {
            log.info("Completed with " + successCopy + " successful copies, and " + errorCopy + " copy errors");
        }
    }

    private void process(File file) throws IOException {

        // get the target HDFS file
        //
        Path targetHdfsFile = getHdfsTargetPath(file);

        log.info("Copying local file '" + file.getAbsolutePath() + "' to HDFS location '" + targetHdfsFile + "'");

        if (dryRun) {
            return;
        }

        // if the directory of the target HDFS file doesn't exist, attempt to create it
        //
        Path parentHdfsDir = targetHdfsFile.getParent();
        if (!hdfsFs.exists(parentHdfsDir)) {
            log.info("Attempting creation of HDFS target directory: " + parentHdfsDir);
            if (!hdfsFs.mkdirs(parentHdfsDir)) {
                log.error("Failed to create target directory in HDFS: " + parentHdfsDir);
                return;
            }
        }

        // copy the file
        //
        BufferedInputStream is = null;
        OutputStream os = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            os = hdfsFs.create(targetHdfsFile);

            if (codec != null) {
                os = codec.createOutputStream(os);
            }

            IOUtils.copyBytes(is, os, 4096, true);
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(os);
        }

        if(file.length() != hdfsFs.getFileStatus(targetHdfsFile).getLen()) {
            throw new IOException("File sizes don't match, local = " + file.length() + ", HDFS = " +
            hdfsFs.getFileStatus(targetHdfsFile).getLen());
        }

        // remove or move the file away from the source directory
        //
        if(remove) {
            if(!file.delete()) {
                log.warn("Failed to delete file '" + file.getAbsolutePath() + "'");
            }
        } else {
            File dest = new File(localCompleteDir, file.getName());
            if(!file.renameTo(dest)) {
                log.warn("Failed to move file from '" + file.getAbsolutePath() + "' to '" + dest.getAbsolutePath() + "'");
            }
        }
    }

    private Path getHdfsTargetPath(File localSrc) throws IOException {
        if(hdfsDestDir != null) {
            if(codec != null) {
                return new Path(hdfsDestDir, localSrc.getName() + codec.getDefaultExtension());
            } else {
                return new Path(hdfsDestDir, localSrc.getName());
            }
        } else {
            return new Path(ScriptExecutor.getHdfsTargetFileFromScript(script, localSrc, 60, TimeUnit.SECONDS));
        }
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
