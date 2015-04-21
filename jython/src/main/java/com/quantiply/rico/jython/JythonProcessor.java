package com.quantiply.rico.jython;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import com.quantiply.rico.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.io.*;
import java.util.List;

public class JythonProcessor implements Processor {

    private final static Logger LOG = LoggerFactory.getLogger(JythonProcessor.class);
    private static String ENGINE = "python";
    private Invocable _py;
    Configuration _cfg;

    @Override
    public void init(Configuration cfg, Context context) throws Exception {
        _cfg = cfg;
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName(ENGINE);
        if (engine == null) {
            throw new RuntimeException("Jython engine not found. Make sure that Jython jar in on the classpath.");
        }
        String scriptPath = cfg.getString("file");

        SimpleScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute(ScriptEngine.FILENAME, scriptPath, ScriptContext.ENGINE_SCOPE);
        engine.setContext(scriptContext);

        File scriptFile = new File(scriptPath);
        if (!scriptFile.exists()) {
            throw new FileNotFoundException("Script File [" + scriptPath + "] not found!");
        }

        engine.eval(new FileReader(scriptFile));
        _py = (Invocable) engine;

        try {
            _py.invokeFunction("init", context);
        } catch (NoSuchMethodException ex) {
           LOG.warn("init function not found in " + scriptPath +" .");
        }


    }

    @Override
    public Envelope process(Envelope event) throws Exception {
        Object res = _py.invokeFunction("process", event);
        return (Envelope) res;
    }

    @Override
    public List window() throws Exception {
        return null;
    }


    @Override
    public void shutdown() throws Exception {

    }
}
