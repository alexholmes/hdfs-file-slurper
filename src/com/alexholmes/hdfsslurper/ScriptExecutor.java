package com.alexholmes.hdfsslurper;

import org.apache.commons.exec.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ScriptExecutor {
    private static Log log = LogFactory.getLog(ScriptExecutor.class);

    public static String getHdfsTargetFileFromScript(String script, File localSrcFile, int timeout, TimeUnit timeoutUnit)
            throws IOException {

        log.info(localSrcFile.getAbsolutePath());

        //CommandLine commandLine = new CommandLine("/bin/bash -c " + script);
        CommandLine commandLine = new CommandLine(script);

        // create the executor and consider the exitValue '1' as success
        Executor executor = new DefaultExecutor();
        executor.setExitValue(0);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayInputStream bais = new ByteArrayInputStream((localSrcFile.getAbsolutePath() + "\n").getBytes());
        PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(baos, System.err, bais);
        executor.setStreamHandler(pumpStreamHandler);

        // create a watchdog
        //
        ExecuteWatchdog watchdog = new ExecuteWatchdog(timeoutUnit.toMillis(timeout));
        executor.setWatchdog(watchdog);

        log.info("Launching script '" + script + "' with local source file '" + localSrcFile.getAbsolutePath() + "'");
        int exitCode = executor.execute(commandLine);

        if(exitCode != 0) {
            throw new IOException("Script exited with non-zero exit code " + exitCode);
        }

        if(watchdog.killedProcess()) {
            throw new IOException("Watchdog had to kill script process");
        }

        String hdfsTargetFile = StringUtils.trim(baos.toString());

        if(StringUtils.isBlank(hdfsTargetFile)) {
            throw new IOException("Received empty HDFS destination file from script");
        }

        return hdfsTargetFile;
    }
}
