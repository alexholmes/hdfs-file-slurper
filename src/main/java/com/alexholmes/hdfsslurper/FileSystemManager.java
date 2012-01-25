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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FileSystemManager {
    private static Log log = LogFactory.getLog(FileSystemManager.class);

    private final Config config;

    private final ReentrantLock inboundDirLock = new ReentrantLock();

    public FileSystemManager(Config config) throws IOException {
      this.config = config;
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
            for (FileStatus fs : config.getSrcFs().listStatus(config.getSrcDir())) {
                if (!fs.isDir()) {
                    if (fs.getPath().getName().startsWith(".")) {
                        log.debug("Ignoring hidden file '" + fs.getPath() + "'");
                        continue;
                    }

                    // move file into work directory
                    //
                    Path workPath = new Path(config.getWorkDir(), fs.getPath().getName());
                  config.getSrcFs().rename(fs.getPath(), workPath);

                    return config.getSrcFs().getFileStatus(workPath);
                }
            }
            return null;
        } finally {
            inboundDirLock.unlock();
        }
    }

    public boolean fileCopyComplete(FileStatus fs) throws IOException {
        boolean success;
        if (config.isRemove()) {
            log.info("File copy successful, deleting source " + fs.getPath());
            success = config.getSrcFs().delete(fs.getPath(), false);
            if(!success) {
                log.info("File deletion unsuccessful");
            }
        } else {
            Path completedPath = new Path(config.getCompleteDir(), fs.getPath().getName());
            log.info("File copy successful, moving source " + fs.getPath() + " to completed file " + completedPath);
            success = config.getSrcFs().rename(fs.getPath(), completedPath);
            if(!success) {
                log.info("File move unsuccessful");
            }
        }
        return success;
    }

    public boolean fileCopyError(FileStatus fs) throws IOException, InterruptedException {
        Path errorPath = new Path(config.getErrorDir(), fs.getPath().getName());
        log.info("Found file in work directory, moving " + fs.getPath() + " to error file " + errorPath);
        return config.getSrcFs().rename(fs.getPath(), errorPath);
    }

    public void moveWorkFilesToError() throws IOException, InterruptedException {
        for (FileStatus fs : config.getSrcFs().listStatus(config.getWorkDir())) {
            if (!fs.isDir()) {
                if (fs.getPath().getName().startsWith(".")) {
                    log.debug("Ignoring hidden file '" + fs.getPath() + "'");
                    continue;
                }

                fileCopyError(fs);
            }
        }
    }

    public Path getStagingFile(FileStatus srcFileStatus, Path destFile) {
        int hash = Math.abs((srcFileStatus.getPath().toString() + destFile.toString()).hashCode() + new Random().nextInt());
        return new Path(config.getDestStagingDir(), String.valueOf(hash));
    }
}
