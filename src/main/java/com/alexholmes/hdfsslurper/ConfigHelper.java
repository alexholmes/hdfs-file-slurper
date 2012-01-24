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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigHelper {

    private static Log log = LogFactory.getLog(ConfigHelper.class);

    public static Map<String, String> loadProperties(String path) throws IOException {
        InputStream is = new FileInputStream(path);
        Properties properties = new Properties();
        properties.load(is);
        is.close();

        Map<String, String> lowercaseProps = new HashMap<String, String>();
        for (Map.Entry entry : properties.entrySet()) {
            lowercaseProps.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
        }
        return lowercaseProps;
    }

    public static String getConfigValue(Map<String, String> props, CommandLine commandLine, String key) {
        key = key.toLowerCase();
        if (commandLine.hasOption(key)) {
            if(props != null && props.containsKey(key)) {
                log.info("Command-line value for " + key + " '" +
                        commandLine.getOptionValue(key) + "' is overriding value in config file of '" +
                        props.get(key) + "'");
            }
            return commandLine.getOptionValue(key);
        }
        if (props != null && props.containsKey(key)) {
            return props.get(key);
        }
        return null;
    }

    public static String getRequiredConfigValue(Map<String, String> props, CommandLine commandLine, String key) throws MissingRequiredConfigException {
        String val = getConfigValue(props, commandLine, key);
        if (val == null) {
            throw new MissingRequiredConfigException(key);
        }
        return val;
    }

    public static boolean isOptionEnabled(Map<String, String> props, CommandLine commandLine, String key) {
        return commandLine.hasOption(key) ||
                props != null &&
                        props.containsKey(key.toLowerCase()) &&
                        "true".equals(props.get(key));
    }

    public static class MissingRequiredConfigException extends Throwable {
        private final String key;

        public MissingRequiredConfigException(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }
}
