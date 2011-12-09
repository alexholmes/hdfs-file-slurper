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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FileSystemManager {
    private static Log log = LogFactory.getLog(FileSystemManager.class);

    private final Configuration config;
    private final Path inboundDirectory;
    private final Path workDirectory;
    private final Path completedDirectory;
    private final Path errorDirectory;
    private final Path destinationDirectory;
    private final boolean removeFileAfterCopy;
    private final FileSystem sourceFileSystem;

    private final ReentrantLock inboundDirLock = new ReentrantLock();

    public FileSystemManager(Configuration conf, Path inboundDirectory, Path workDirectory, Path completedDirectory, Path errorDirectory, Path destinationDirectory, boolean removeFileAfterCopy) throws IOException {
        this.config = conf;
        this.inboundDirectory = inboundDirectory;
        this.workDirectory = workDirectory;
        this.completedDirectory = completedDirectory;
        this.errorDirectory = errorDirectory;
        this.destinationDirectory = destinationDirectory;
        this.removeFileAfterCopy = removeFileAfterCopy;
        sourceFileSystem = inboundDirectory.getFileSystem(config);
    }

    public FileStatus pollForInboundFile(TimeUnit unit, long period) throws IOException, InterruptedException {
        FileStatus fs;
        // we should optimize fo the case where the FileSystem is a local file sytem,
        // in which case we could use Java 7's WatchService
        //
        while ((fs = getInboundFile()) == null) {
            unit.sleep(period);
        }
        return fs;
    }

    public FileStatus getInboundFile() throws IOException, InterruptedException {
        try {
            inboundDirLock.lockInterruptibly();
            for (FileStatus fs : sourceFileSystem.listStatus(inboundDirectory)) {
                if (!fs.isDir()) {
                    if (fs.getPath().getName().startsWith(".")) {
                        log.debug("Ignoring hidden file '" + fs.getPath() + "'");
                        continue;
                    }

                    // move file into work directory
                    //
                    Path workPath = new Path(workDirectory, fs.getPath().getName());
                    sourceFileSystem.rename(fs.getPath(), workPath);

                    return sourceFileSystem.getFileStatus(workPath);
                }
            }
            return null;
        } finally {
            inboundDirLock.unlock();
        }
    }

    public boolean fileCopyComplete(FileStatus fs) throws IOException {
        boolean success;
        if (removeFileAfterCopy) {
            log.info("File copy successful, deleting source " + fs.getPath());
            success = sourceFileSystem.delete(fs.getPath(), false);
            if(!success) {
                log.info("File deletion unsuccessful");
            }
        } else {
            Path completedPath = new Path(completedDirectory, fs.getPath().getName());
            log.info("File copy successful, moving source " + fs.getPath() + " to completed file " + completedPath);
            success = sourceFileSystem.rename(fs.getPath(), completedPath);
            if(!success) {
                log.info("File move unsuccessful");
            }
        }
        return success;
    }

    public boolean fileCopyError(FileStatus fs) throws IOException, InterruptedException {
        Path errorPath = new Path(errorDirectory, fs.getPath().getName());
        log.info("File copy error, moving source " + fs.getPath() + " to error file " + errorPath);
        return sourceFileSystem.rename(fs.getPath(), errorPath);
    }

    public Path getInboundDirectory() {
        return inboundDirectory;
    }

    public Path getWorkDirectory() {
        return workDirectory;
    }

    public Path getCompletedDirectory() {
        return completedDirectory;
    }

    public boolean isRemoveFileAfterCopy() {
        return removeFileAfterCopy;
    }

    public Path getErrorDirectory() {
        return errorDirectory;
    }

    public Path getDestinationDirectory() {
        return destinationDirectory;
    }

    public void moveWorkFilesToError() throws IOException, InterruptedException {
        for (FileStatus fs : sourceFileSystem.listStatus(workDirectory)) {
            if (!fs.isDir()) {
                if (fs.getPath().getName().startsWith(".")) {
                    log.debug("Ignoring hidden file '" + fs.getPath() + "'");
                    continue;
                }

                fileCopyError(fs);
            }
        }
    }
}
