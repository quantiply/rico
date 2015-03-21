package com.quantiply.local;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantiply.rico.api.Configuration;
import com.quantiply.rico.core.Configurator;
import com.quantiply.rico.api.Envelope;
import com.quantiply.rico.api.Processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroRunnable {
    private final ObjectMapper _mapper;
    private int BATCH_SIZE ;
    private List<Envelope<?>> _localCache;
    private Processor _task;
    private boolean isWindowTriggered;

    public AvroRunnable(String configPath) throws Exception {
        _localCache = new ArrayList<>();

        JsonFactory factory = new JsonFactory();
        _mapper = new ObjectMapper(factory);

        // Read the config.
        Configurator cfg = new Configurator(configPath);

        Configuration localCfg = cfg.get("local");
        // Instantiate the processor.
        String processorClass = localCfg.getString("processor.class");
        String processorName = localCfg.getString("processor.name");
        BATCH_SIZE = localCfg.getInt("processor.batch.size");

        Class clazz = Class.forName(processorClass);
        _task = (Processor) clazz.newInstance();
        _task.init(cfg.get(processorName), null);

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

        while((input=br.readLine())!=null) {
//            System.out.println(input);

            // Convert string to Map.
            TypeReference<HashMap<String,Object>> typeRef
                    = new TypeReference<HashMap<String,Object>>() {};

            Map<String,Object> json = _mapper.readValue(input, typeRef);

            // Add envelope if it is not present.
            Envelope<Object> event = new Envelope<>();

            if(json.containsKey("headers")) {
                event.setHeaders((Map<String, String>) json.get("headers"));
            }

            if(json.containsKey("payload")){
                event.setPayload(json.get("payload"));
            } else {
                event.setPayload(json);
            }

            // Add to Buffer
            _localCache.add(event);

            if (isWindowTriggered) {
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
                // Convert Envelope to JSON string.
                Map<String, Object> event = new HashMap<String, Object>();
                event.put("headers", result.getHeaders());
                event.put("payload", result.getPayload());
//                System.out.println("Event :" + event);
                StringWriter stringWriter = new StringWriter();

                try {
                    _mapper.writeValue(stringWriter, event);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(stringWriter.toString());
            });
        }
    }
}
