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

import org.apache.samza.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

import javax.script.*;
import java.io.FileReader;
import java.net.URLDecoder;

public class JythonTask implements StreamTask, InitableTask, WindowableTask, ClosableTask {
    public static final String CONFIG_JYTHON_ENTRYPOINT = "rico.jython.entrypoint";
    private static final String ENGINE = "python";
    private static final Logger logger = LoggerFactory.getLogger(JythonTask.class);
    private Invocable py;
    private PyTask task;

    @Override
    public void init(Config config, TaskContext context) throws Exception {

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName(ENGINE);

        if (engine == null) {
            throw new RuntimeException("Jython engine not found. Make sure that Jython jar in on the classpath.");
        }

        logger.debug("ENV : " + System.getenv());
        logger.debug("System properties : " + System.getProperties());
        logger.debug("JAR dir : " + getJarDir());
        logger.info("APP HOME (calculated in Java): " + getAppHome());
        logger.debug("Config : " + config);

        // Setup python path

        // The following ensures that we get filename correctly in error messages.
        String scriptPath = getAppHome() + "/lib/rico/bootstrap.py";
        SimpleScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute(ScriptEngine.FILENAME, scriptPath, ScriptContext.ENGINE_SCOPE);
        engine.setContext(scriptContext);
        py = (Invocable) engine;

        engine.eval(new FileReader(getAppHome() + "/lib/rico/bootstrap.py"));
        py.invokeFunction("bootstrap", getAppHome());

        // Instantiate the Python class and cast it to the Task interface. Duck typing FTW !!
        String pyClass = config.get(CONFIG_JYTHON_ENTRYPOINT);
        if (pyClass == null) {
            throw new ConfigException("Jython entry point not configured. Missing property: " + CONFIG_JYTHON_ENTRYPOINT);
        }
        py.invokeFunction("create_entrypoint", pyClass);
        task = py.getInterface(engine.get("com_quantiply_rico_entrypoint"), PyTask.class);
        task.init(config, context);
    }

    private String getAppHome() throws Exception {
        return getJarDir().substring(0, getJarDir().lastIndexOf("/"));
    }

    private String getJarDir() throws Exception {
        String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
//        absolutePath = URLDecoder.decode(absolutePath, "UTF-8");
        absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
        absolutePath = absolutePath.replaceAll("%20", " "); // Surely need to do this here
        return absolutePath;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        task.process(envelope, collector, coordinator);
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        task.window(collector, coordinator);
    }

    @Override
    public void close() throws Exception {
        if (task != null) {
            task.close();
        }
    }

}

