package com.alexholmes.hdfsslurper;

import org.apache.commons.exec.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ScriptExecutor {
    private static Log log = LogFactory.getLog(ScriptExecutor.class);

    public static String getStdOutFromScript(String script, String stdInLine, int timeout, TimeUnit timeoutUnit)
            throws IOException {

        String[] execAndArgs = splitArgs(script);

        CommandLine commandLine = new CommandLine(execAndArgs[0]);

        if(execAndArgs.length > 1) {
            commandLine.addArguments(Arrays.copyOfRange(execAndArgs, 1, execAndArgs.length));
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

    final static int OUTSIDE = 1;
    final static int SINGLEQ = 2;
    final static int DOUBLEQ = 3;

    static String[] splitArgs(String args) {
        ArrayList argList = new ArrayList();
        char[] ch = args.toCharArray();
        int clen = ch.length;
        int state = OUTSIDE;
        int argstart = 0;
        for (int c = 0; c <= clen; c++) {
            boolean last = (c == clen);
            int lastState = state;
            boolean endToken = false;
            if (!last) {
                if (ch[c] == '\'') {
                    if (state == OUTSIDE) {
                        state = SINGLEQ;
                    } else if (state == SINGLEQ) {
                        state = OUTSIDE;
                    }
                    endToken = (state != lastState);
                } else if (ch[c] == '"') {
                    if (state == OUTSIDE) {
                        state = DOUBLEQ;
                    } else if (state == DOUBLEQ) {
                        state = OUTSIDE;
                    }
                    endToken = (state != lastState);
                } else if (ch[c] == ' ') {
                    if (state == OUTSIDE) {
                        endToken = true;
                    }
                }
            }
            if (last || endToken) {
                if (c == argstart) {
                    // unquoted space
                } else {
                    String a;
                    a = args.substring(argstart, c);
                    argList.add(a);
                }
                argstart = c + 1;
                lastState = state;
            }
        }
        return (String[]) argList.toArray(new String[0]);
    }
}
