package com.alexholmes.hdfsslurper;

import org.apache.commons.exec.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ScriptExecutor {
    private static Log log = LogFactory.getLog(ScriptExecutor.class);

    public static String getStdOutFromScript(String script, String stdInLine, int timeout, TimeUnit timeoutUnit)
            throws IOException {

        //CommandLine commandLine = new CommandLine("/bin/bash -c " + script);

        String[] execAndArgs = StringUtils.split(script, " ", 2);

        CommandLine commandLine = new CommandLine(execAndArgs[0]);

        if(execAndArgs.length > 1) {
            commandLine.addArguments(StringUtils.split(execAndArgs[1], " "));
        }

        // create the executor and consider the exitValue '1' as success
        Executor executor = new DefaultExecutor();
        executor.setExitValue(0);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayOutputStream baes = new ByteArrayOutputStream();
        ByteArrayInputStream bais = new ByteArrayInputStream((stdInLine + "\n").getBytes());
        PumpStreamHandler pumpStreamHandler = new PumpStreamHandler(baos, baes, bais);
        executor.setStreamHandler(pumpStreamHandler);

        // create a watchdog
        //
        ExecuteWatchdog watchdog = new ExecuteWatchdog(timeoutUnit.toMillis(timeout));
        executor.setWatchdog(watchdog);

        log.info("Launching script '" + script + "' and piping the following to stdin '" + stdInLine + "'");

        try {
            executor.execute(commandLine);
        } catch(IOException e) {
            log.error("Script exited with non-zero exit code");
            log.error("Stdout = ");
            log.error(baos.toString());
            log.error("Stderr = ");
            log.error(baes.toString());
            throw e;
        }


        if(watchdog.killedProcess()) {
            throw new IOException("Watchdog had to kill script process");
        }

        String hdfsTargetFile = StringUtils.trim(baos.toString());

        if(StringUtils.isBlank(hdfsTargetFile)) {
            throw new IOException("Received empty stdout from script");
        }

        return hdfsTargetFile;
    }
}
