package com.quantiply.rico.samza;

import com.quantiply.rico.Configurator;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import org.apache.log4j.Logger;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SamzaRunner implements StreamTask, InitableTask, WindowableTask, ClosableTask {

    private final static Logger LOG = Logger.getLogger(SamzaRunner.class);

    private int BATCH_SIZE ;
    private List<Envelope> _localCache;
    private SystemStream _output;
    private SystemStream _error;
    private Processor _task;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        _localCache = new ArrayList<>();

        // TODO: Support multiple outputs
        _output = new SystemStream("kafka", config.get("task.output"));
        _error = new SystemStream("kafka", config.get("task.error"));

        // See comment in main() below.
        Configurator cfg = new Configurator(config.get("samza.runner.config.path"));

        // Run the processor.
        String processorClass = config.get("processor.class");
        String processorName = config.get("processor.name");
        BATCH_SIZE = config.getInt("processor.batch.size", 1000);

        Class clazz = Class.forName(processorClass);
        _task = (Processor) clazz.newInstance();

        _task.init(cfg.get(processorName), new SamzaContext(context));
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Object message =  envelope.getMessage();

        Envelope event = new Envelope();
        event.setBody(message);
        event.setHeader("offset", envelope.getOffset());
        event.setHeader("key", (String) envelope.getKey());
        event.setHeader("system", envelope.getSystemStreamPartition().getSystem());
        event.setHeader("stream", envelope.getSystemStreamPartition().getStream());
        event.setHeader("partition", "" + envelope.getSystemStreamPartition().getPartition().getPartitionId());

        // Add to Buffer
        _localCache.add(event);
        if (_localCache.size() >= BATCH_SIZE) {
            List<Envelope> results = _task.process(_localCache);
            send(results, collector);
            _localCache.clear();
        }
    }


    private void send(List<Envelope> results, MessageCollector collector){
        if(results != null) {
            for(Envelope result: results){
                // TODO : Add output routing here. Add topic and partition details
                // Maybe do a send(data, to, groupby_function, primarykey_function) -> send(data) method which encapsulates this.
                if(result.isError()){
                    // TODO: Not sure what kind of serialization to use here. Maybe JSON is better.
                    collector.send(new OutgoingMessageEnvelope(_error, result));
                } else {
                    collector.send(new OutgoingMessageEnvelope(_output, result.getBody()));
                }
            }
        }
    }
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        List<Envelope> results = _task.window();
        if(results != null) {
            send(results, collector);
        }
    }

    @Override
    public void close() throws Exception {
        // Guard against the edge case where close is called before init().
        if(_task != null ) {
           _task.shutdown();
       }
    }

    public static void main(String[] args) throws Exception {

        String defaultsPath = "defaults.yml";
        LOG.info("Reading default config from : " + defaultsPath);
        String configPath = args[0];
        LOG.info("Reading config from : " + configPath);

        Configurator cfg = new Configurator(defaultsPath, configPath);
        Map<String, String> samzaConfig = cfg.getAsStringMap("samza");
        // There is no way to pass config elegantly as the Samza config only accepts String values and is IMMUTABLE.
        // So either we append the Quantiply task properties keys with the section name and create a map which is only scoped
        // to that subset or we can make it simple and reread the properties when we instantiate the task.
        // I am taking the simpler route until it becomes clear that it is not the right choice.
        samzaConfig.put("samza.runner.config.path", configPath);

        LOG.info("Samza config : " + samzaConfig);
        // Samza song and dance.
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(samzaConfig);
        new JobRunner(new MapConfig(configs)).run();

    }
}
