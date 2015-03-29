package com.quantiply.rico.jython;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

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

        String pyClass = cfg.getString("pyClass");
        System.out.println("PyClass = " + pyClass);
        engine.eval("c = " + pyClass + "()");
        _processor = _py.getInterface(engine.get("c"), Processor.class);
        _processor.init(cfg, context);
    }

    @Override
    public List<Envelope> process(List<Envelope> events) throws Exception {
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
}
