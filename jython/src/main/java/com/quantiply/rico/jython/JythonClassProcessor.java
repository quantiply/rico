package com.quantiply.rico.jython;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

public class JythonClassProcessor implements Processor {

    private final static Logger LOG = LoggerFactory.getLogger(JythonClassProcessor.class);
    private static String ENGINE = "python";
    private Invocable _py;
    Configuration _cfg;
    private Processor _processor;

    @Override
    public void init(Configuration cfg, Context context) throws Exception {
        _cfg = cfg;
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName(ENGINE);
        if (engine == null) {
            throw new RuntimeException("Jython engine not found. Make sure that Jython jar in on the classpath.");
        }

        LOG.debug("ENV : " + System.getenv());
        LOG.debug("System properties : "+ System.getProperties());
        LOG.debug("JAR dir : " + getJarDir());
        LOG.info("APP HOME (calculated in Java): " + getAppHome());
        LOG.debug("Config : " + _cfg);

        // Setup python path

        // The following ensures that we get filename correctly in error messages.
        String scriptPath = getAppHome() + "/lib/rico/bootstrap.py";
        SimpleScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute(ScriptEngine.FILENAME, scriptPath, ScriptContext.ENGINE_SCOPE);
        engine.setContext(scriptContext);
        _py = (Invocable) engine;

        engine.eval(new FileReader(getAppHome() + "/lib/rico/bootstrap.py"));
        _py.invokeFunction("bootstrap", getAppHome());

        // Instantiate the Python class and cast it to the Processor interface. Duck typing FTW !!
        String pyClass = cfg.getString("processor.entrypoint");
        _py.invokeFunction("create_entrypoint", pyClass);
        _processor = _py.getInterface(engine.get("com_quantiply_rico_entrypoint"), Processor.class);
        _processor.init(cfg, context);
    }

    @Override
    public Envelope process(Envelope events) throws Exception {
        return _processor.process(events);
    }

    @Override
    public List window() throws Exception {
        return _processor.window();
    }


    @Override
    public void shutdown() throws Exception {
        _processor.shutdown();
    }


    private String getAppHome(){
        return getJarDir().substring(0, getJarDir().lastIndexOf("/"));
    }
    private String getJarDir(){
        String absolutePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
        absolutePath = absolutePath.replaceAll("%20"," "); // Surely need to do this here
        return absolutePath;
    }
}
