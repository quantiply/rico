package com.quantiply.rico.local;

import com.quantiply.rico.*;
import com.quantiply.rico.serde.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class LocalRunner {
    private final static Logger LOG = LoggerFactory.getLogger(LocalRunner.class);
    private final String _configPath;
    private StringSerde _serde;
    private Processor _task;
    private boolean _isWindowTriggered;

    public LocalRunner(String configPath) {
        _configPath = configPath;
    }

    public void init() throws Exception {
        Class clazz;
        // Read the config.
        Configuration localCfg = new Configuration(LocalUtils.getAll(_configPath));

        LOG.debug("Local Config :" + localCfg);

        // Initialize Serde
        String serdeClass = localCfg.getString("string-serde.class");
        clazz = Class.forName(serdeClass);
        _serde = (StringSerde) clazz.newInstance();
        _serde.init(localCfg);

        // Instantiate the processor
        String processorClass = localCfg.getString("processor.class");
        String processorName = localCfg.getString("processor.name");

        LOG.debug("Processor [%s] Config : %s".format(processorName, localCfg));

        Context context = new LocalContext(localCfg);
        clazz = Class.forName(processorClass);
        _task = (Processor) clazz.newInstance();
        _task.init(localCfg, context);

        // TODO: Add a timer for window and handle it below.
        _isWindowTriggered = false;
    }

    public void close() throws Exception {
        _task.shutdown();
    }

    public void run() throws Exception {
        // Read from STDIN
        BufferedReader br =
                new BufferedReader(new InputStreamReader(System.in));

        String input;

        while ((input = br.readLine()) != null) {
            Envelope event = _serde.fromString(input);
            output(_task.process(event));
        }
    }

    private void output(Envelope result) {
        if (result != null) {
            StringWriter stringWriter = new StringWriter();
            try {
                _serde.writeTo(result, stringWriter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println(stringWriter.toString());
        }
    }

    public static void main(String[] args) {
        try {
            String configPath = args[0];
            LocalRunner runner = new LocalRunner(configPath);
            runner.init();
            runner.run();
        } catch (ScriptException e) {
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
//            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected exception in LocalRunner" + e.getMessage());
            e.printStackTrace();
        }

        // TODO: Handle Ctrl + C
    }
}
