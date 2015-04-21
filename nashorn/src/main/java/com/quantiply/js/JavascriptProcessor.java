package com.quantiply.js;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import com.quantiply.rico.Context;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JavascriptProcessor implements Processor {

    private static String JS_ENGINE = "nashorn";
    private Invocable _js;
    private String scriptFullPath;
    Configuration _cfg ;

    @Override
    public void init(Configuration cfg, Context context) throws Exception {
        _cfg = cfg;
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName(JS_ENGINE);
        if (engine == null) {
            throw new RuntimeException("Nashorn JS engine not found. Make sure you are using JDK 8.");
        }
        scriptFullPath = cfg.getString("file");
        SimpleScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute(ScriptEngine.FILENAME, scriptFullPath, ScriptContext.ENGINE_SCOPE);

        engine.setContext(scriptContext);
        File scriptFile = new File(scriptFullPath);
        if (!scriptFile.exists()) {
            throw new FileNotFoundException("Script File [" + scriptFullPath + "] not found!");
        }

//        System.out.println("Script file: " + scriptFullPath);
        engine.eval("var global = this;var window =this;");
        engine.eval(new FileReader(scriptFile));
        String _process = String.format("var process = %s.process;", cfg.getString("processor"));
//        System.out.println(_process);
        engine.eval(_process);
//        engine.eval("print(test.process)");
        _js = (Invocable) engine;

    }

    @Override
    public Envelope process(Envelope events) throws Exception {
        Envelope res = (Envelope) _js.invokeFunction("process", events);
        return res;
    }

    @Override
    public List window() throws Exception {
        return null;
    }


    @Override
    public void shutdown() throws Exception {

    }
}
