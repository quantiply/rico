package com.quantiply.rico.local;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import com.quantiply.rico.Configurator;
import com.quantiply.rico.serde.JSONStringSerde;
import com.quantiply.rico.serde.StringSerde;

import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class LocalRunner {
    private final String _configPath;
    private final List<Envelope<?>> _localCache = new ArrayList<>();
    private StringSerde<Object> _serde;
    private int BATCH_SIZE ;
    private Processor _task;
    private boolean _isWindowTriggered;

    public LocalRunner(String configPath) {
        _configPath = configPath;
    }

    public void init() throws Exception {
        Class clazz;
        // Read the config.
        Configurator cfg = new Configurator(_configPath);

        Configuration localCfg = cfg.get("local");

        System.out.println("Local Config :" + localCfg);

        _isWindowTriggered = false; //TODO - get this from config?

        // Initialize Serde
        String serdeClass = localCfg.getString("processor.serde");
        clazz = Class.forName(serdeClass);
        _serde = (StringSerde<Object>) clazz.newInstance();
        _serde.init(localCfg);

        // Instantiate the processor
        String processorClass = localCfg.getString("processor.class");
        String processorName = localCfg.getString("processor.name");

        BATCH_SIZE = localCfg.getInt("processor.batch.size");

        System.out.println(processorName + " config :" + cfg.get(processorName));

        LocalContext context = new LocalContext(cfg.get(processorName));
        clazz = Class.forName(processorClass);
        _task = (Processor) clazz.newInstance();
        _task.init(cfg.get(processorName), context);

        // TODO: Add a timer for window.
    }

    public void close() throws Exception {
        _task.shutdown();
    }

    public void run() throws Exception{
        // Read from STDIN
        BufferedReader br =
                new BufferedReader(new InputStreamReader(System.in));

        String input;

        while((input=br.readLine()) != null) {
            Envelope<Object> event = _serde.fromString(input);
            _localCache.add(event);

            if (_isWindowTriggered) {
                output(_task.window());
            }

            if (_localCache.size() >= BATCH_SIZE) {
                output(_task.process(_localCache));
                _localCache.clear();
            }
        }
    }

    private void output(List<Envelope<Object>> results) {
        if (results != null) {
            results.stream().forEach((result) -> {
                StringWriter stringWriter = new StringWriter();
                try {
                    _serde.writeTo(result, stringWriter);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println(stringWriter.toString());
            });
        }
    }

    public static void main(String[] args) {
        try {
            String configPath = args[0];
            LocalRunner runner = new LocalRunner(configPath);
            runner.init();
            runner.run();
        } catch (Exception e) {
            if(! (e instanceof ScriptException)){
                e.printStackTrace();
            } else {
                System.out.println(e.getMessage());
            }
        }

        // TODO: Handle Ctrl + C
    }
}
