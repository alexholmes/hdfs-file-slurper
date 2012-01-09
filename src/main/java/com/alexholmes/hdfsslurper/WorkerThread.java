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

import com.hadoop.compression.lzo.LzoIndexer;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.MDC;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class WorkerThread extends Thread {
    private static Log log = LogFactory.getLog(WorkerThread.class);
    private AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final Configuration config;
    private final boolean verifyCopy;
    private final boolean createDoneFile;
    private final String scriptFile;
    private final String workScriptFile;
    private final CompressionCodec codec;
    private final boolean createLzopIndex;
    private final FileSystemManager fileSystemManager;
    private final boolean poll;
    private final TimeUnit pollSleepUnit;
    private final long pollSleepPeriod;
    private final LzoIndexer indexer;

    public WorkerThread(Configuration config,
                        boolean verifyCopy,
                        boolean createDoneFile,
                        String scriptFile,
                        String workScriptFile,
                        CompressionCodec codec,
                        boolean createLzopIndex,
                        FileSystemManager fileSystemManager,
                        int threadIndex,
                        boolean poll,
                        TimeUnit pollSleepUnit,
                        long pollSleepPeriod) {
        this.config = config;
        this.verifyCopy = verifyCopy;
        this.createDoneFile = createDoneFile;
        this.scriptFile = scriptFile;
        this.workScriptFile = workScriptFile;
        this.codec = codec;
        this.createLzopIndex = createLzopIndex;
        this.fileSystemManager = fileSystemManager;
        this.poll = poll;
        this.pollSleepUnit = pollSleepUnit;
        this.pollSleepPeriod = pollSleepPeriod;
        this.setDaemon(true);
        this.setName(WorkerThread.class.getSimpleName() + "-" + threadIndex);
        this.indexer = new LzoIndexer(config);
    }

    @Override
    public void run() {
        MDC.put("threadName", this.getName());
        try {
            while (!shuttingDown.get() && !interrupted()) {
                doWork();
            }
        } catch (InterruptedException t) {
            log.warn("Caught interrupted exception, exiting");
        }
        log.info("Thread exiting");
    }

    private void doWork() throws InterruptedException {
        try {
            if (poll) {
                copyFile(fileSystemManager.pollForInboundFile(pollSleepUnit, pollSleepPeriod));
            } else {
                FileStatus fs = fileSystemManager.getInboundFile();
                if (fs == null) {
                    shuttingDown.set(true);
                } else {
                    copyFile(fs);
                }
            }
        } catch (InterruptedException ie) {
            throw ie;
        } catch (Throwable t) {
            log.warn("Caught exception in doWork", t);
        }
    }

    private synchronized void copyFile(FileStatus fs) throws IOException, InterruptedException {
        if (!shuttingDown.get() && !interrupted()) {
            try {
                process(fs);
            } catch (Throwable t) {
                log.warn("Caught exception working on file " + fs.getPath(), t);
                fileSystemManager.fileCopyError(fs);
            }
        }
    }

    private void process(FileStatus srcFileStatus) throws IOException, InterruptedException {

        FileSystem srcFs = srcFileStatus.getPath().getFileSystem(config);

        // run a script which can change the name of the file as well as write out a new version of the file
        //
        if (workScriptFile != null) {
            Path newSrcFile = stageSource(srcFileStatus);
            srcFileStatus = srcFileStatus.getPath().getFileSystem(config).getFileStatus(newSrcFile);
        }

        Path srcFile = srcFileStatus.getPath();

        // get the target HDFS file
        //
        Path destFile = getHdfsTargetPath(srcFileStatus);

        FileSystem destFs = destFile.getFileSystem(config);

        // get the staging HDFS file
        //
        Path stagingFile = fileSystemManager.getStagingFile(srcFileStatus, destFile);

        log.info("Copying source file '" + srcFile + "' to staging destination '" + stagingFile + "'");

        // if the directory of the target file doesn't exist, attempt to create it
        //
        Path destParentDir = destFile.getParent();
        if (!destFs.exists(destParentDir)) {
            log.info("Attempting creation of target directory: " + destParentDir.toUri());
            if (!destFs.mkdirs(destParentDir)) {
                throw new IOException("Failed to create target directory: " + destParentDir.toUri());
            }
        }

        // if the staging directory doesn't exist, attempt to create it
        //
        Path destStagingParentDir = stagingFile.getParent();
        if (!destFs.exists(destStagingParentDir)) {
            log.info("Attempting creation of staging directory: " + destStagingParentDir.toUri());
            if (!destFs.mkdirs(destStagingParentDir)) {
                throw new IOException("Failed to create staging directory: " + destParentDir.toUri());
            }
        }

        // copy the file
        //
        InputStream is = null;
        OutputStream os = null;
        CRC32 crc = new CRC32();
        try {
            is = new BufferedInputStream(srcFs.open(srcFile));
            if (verifyCopy) {
                is = new CheckedInputStream(is, crc);
            }
            os = destFs.create(stagingFile);

            if (codec != null) {
                os = codec.createOutputStream(os);
            }

            IOUtils.copyBytes(is, os, 4096, true);
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(os);
        }

        long srcFileSize = srcFs.getFileStatus(srcFile).getLen();
        long destFileSize = destFs.getFileStatus(stagingFile).getLen();
        if (codec == null && srcFileSize != destFileSize) {
            throw new IOException("File sizes don't match, source = " + srcFileSize + ", dest = " + destFileSize);
        }

        log.info("Local file size = " + srcFileSize + ", HDFS file size = " + destFileSize);

        if (verifyCopy) {
            verify(stagingFile, crc.getValue());
        }

        if (destFs.exists(destFile)) {
            destFs.delete(destFile, false);
        }

        log.info("Moving staging file '" + stagingFile + "' to destination '" + destFile + "'");
        if (!destFs.rename(stagingFile, destFile)) {
            throw new IOException("Failed to rename file");
        }

        if(codec != null && createLzopIndex) {
            indexer.index(destFile);
        }

        if (createDoneFile) {
            Path doneFile = new Path(destFile.getParent(), destFile.getName() + ".done");
            log.info("Touching done file " + doneFile);
            touch(doneFile);
        }

        fileSystemManager.fileCopyComplete(srcFileStatus);
    }

    private Path stageSource(FileStatus srcFile) throws IOException {
        Path p = new Path(ScriptExecutor.getStdOutFromScript(workScriptFile, srcFile.getPath().toString(), 60, TimeUnit.SECONDS));
        if (p.toUri().getScheme() == null) {
            throw new IOException("Work path from script must be a URI with a scheme: '" + p + "'");
        }
        log.info("Staging script returned new file '" + p + " for old " + srcFile.getPath());
        return p;
    }

    private void verify(Path hdfs, long localFileCRC) throws IOException {
        log.info("Verifying files");
        long hdfsCRC = hdfsFileCRC32(hdfs);

        if (localFileCRC != hdfsCRC) {
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
        p.getFileSystem(config).create(p).close();
    }

    private Path getHdfsTargetPath(FileStatus srcFile) throws IOException {
        if (fileSystemManager.getDestinationDirectory() != null) {
            if (codec != null) {
                return new Path(fileSystemManager.getDestinationDirectory(), srcFile.getPath().getName() + codec.getDefaultExtension());
            } else {
                return new Path(fileSystemManager.getDestinationDirectory(), srcFile.getPath().getName());
            }
        } else {
            return getDestPathFromScript(srcFile);
        }
    }

    private Path getDestPathFromScript(FileStatus srcFile) throws IOException {
        Path p = new Path(ScriptExecutor.getStdOutFromScript(scriptFile, srcFile.getPath().toString(), 60, TimeUnit.SECONDS));
        if (p.toUri().getScheme() == null) {
            throw new IOException("Destination path from script must be a URI with a scheme: '" + p + "'");
        }
        return p;
    }

    public synchronized void shutdown() throws InterruptedException {
        if (!shuttingDown.getAndSet(true)) {
            log.info("Interrupting: " + this.getName());
            this.interrupt();
            log.info("Joining: " + this.getName());
            this.join();
        }
    }
}
