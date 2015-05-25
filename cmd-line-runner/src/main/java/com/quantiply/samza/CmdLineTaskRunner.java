/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza;

import com.quantiply.samza.serde.StringSerde;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.factories.PropertiesConfigFactory;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.*;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class CmdLineTaskRunner {
    public final static String CONFIG_SERDE_CLASS = "rico.string-serde.class";
    public final static String CONFIG_TASK_CLASS = "task.class";
    private final static SystemStreamPartition STDIN_SSP = new SystemStreamPartition("stdin", "stdin", new Partition(0));

    private static Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());
    private final String configPath;
    private Object task;
    private boolean isWindowTriggered;
    private StringSerde serde;
    private int verbosity;

    public CmdLineTaskRunner(String configPath, int verbosity) {
        this.verbosity = verbosity;
        this.configPath = configPath;
    }

    public void init() throws Exception {
        // Read the config.
        PropertiesConfigFactory pcf = new PropertiesConfigFactory();
        Config cfg = pcf.getConfig(new URI("file://" + configPath));

        logger.debug("Local Config :" + cfg);

        // Initialize Serde
        String serdeClassName = cfg.get(CONFIG_SERDE_CLASS);
        if (serdeClassName == null) {
            throw new ConfigException("Serde not configured. Missing property: " + CONFIG_SERDE_CLASS);
        }
        Class serdeClass = Class.forName(serdeClassName);
        serde = (StringSerde) serdeClass.newInstance();
        serde.init(cfg);

        // Instantiate the processor
        String taskClass = cfg.get(CONFIG_TASK_CLASS);
        if (taskClass == null) {
            throw new ConfigException("Task class not configured. Missing property: " + CONFIG_TASK_CLASS);
        }
        Class clazz = Class.forName(taskClass);
        task = clazz.newInstance();
        ((InitableTask) task).init(cfg, new LocalTaskContext() );

        // TODO: Add a timer for window and handle it below.
        isWindowTriggered = false;
    }

    public void close() throws Exception {
        ((ClosableTask) task).close();
    }

    public void run() throws Exception {
        int lineOffset = 0;
        // Read from STDIN
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String input;
        while ((input = br.readLine()) != null) {
            Object data = serde.fromString(input);
            ((StreamTask) task).process(new IncomingMessageEnvelope(STDIN_SSP, Integer.toString(lineOffset), null, data), new LocalCollector(verbosity), null);
            lineOffset++;
        }
    }

    class LocalTaskContext implements TaskContext {
        @Override
        public MetricsRegistry getMetricsRegistry() {
            return new NoOpMetricsRegistry();
        }

        @Override
        public Set<SystemStreamPartition> getSystemStreamPartitions() {
            Set<SystemStreamPartition> set = new HashSet<>();
            set.add(STDIN_SSP);
            return set;
        }

        @Override
        public Object getStore(String s) {
            return null;
        }

        @Override
        public TaskName getTaskName() {
            return new TaskName("local");
        }

        @Override
        public void setStartingOffset(SystemStreamPartition systemStreamPartition, String s) {}
    }

    class LocalCollector implements MessageCollector {
        private int type;

        public LocalCollector(int verbosity) {
            this.type = verbosity;
        }

        @Override
        public void send(OutgoingMessageEnvelope outgoingMessageEnvelope) {
            if (type == 0) {
                System.out.println(outgoingMessageEnvelope);
            }
            else {
                try {
                    StringWriter stringWriter = new StringWriter();
                    serde.writeTo(outgoingMessageEnvelope.getMessage(), stringWriter);
                    System.out.println(stringWriter.toString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            String configPath = args[0];
            int isVerbose = Integer.parseInt(args[1]);
            CmdLineTaskRunner runner = new CmdLineTaskRunner(configPath, isVerbose);
            runner.init();
            runner.run();
        } catch (ScriptException e) {
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
        } catch (Exception e) {
            System.err.println("Unexpected exception in CmdLineTaskRunner" + e.getMessage());
            e.printStackTrace();
        }
    }
}
