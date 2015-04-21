package com.quantiply.rico.samza;

import com.quantiply.rico.Configuration;
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

    private SystemStream _output;
    private SystemStream _error;
    private Processor _task;

    @Override
    public void init(Config config, TaskContext context) throws Exception {

        LOG.debug(config.toString());
        // TODO: Support multiple outputs
        _output = new SystemStream("kafka", config.get("task.output").replace("kafka.", ""));
        _error = new SystemStream("kafka", config.get("task.error").replace("kafka.", "'"));

        // Run the processor.
        String processorClass = config.get("processor.class");
        String processorName = config.get("processor.name");

        Class clazz = Class.forName(processorClass);
        _task = (Processor) clazz.newInstance();

        _task.init(new Configuration(config), new SamzaContext(context));
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Object message = envelope.getMessage();

        Envelope event = new Envelope();
        event.setBody(message);
        event.setHeader("offset", envelope.getOffset());
        event.setHeader("key", (String) envelope.getKey());
        event.setHeader("system", envelope.getSystemStreamPartition().getSystem());
        event.setHeader("stream", envelope.getSystemStreamPartition().getStream());
        event.setHeader("partition", "" + envelope.getSystemStreamPartition().getPartition().getPartitionId());

        Envelope result = _task.process(event);
        send(result, collector);
    }


    private void send(Envelope result, MessageCollector collector) {
        if (result != null) {
            // TODO : Add output routing here. Add topic and partition details
            // Maybe do a send(data, to, groupby_function, primarykey_function) -> send(data) method which encapsulates this.
            if (result.isError()) {
                // TODO: Not sure what kind of serialization to use here. Maybe JSON is better.
                collector.send(new OutgoingMessageEnvelope(_error, result));
            } else {
                collector.send(new OutgoingMessageEnvelope(_output, result.getBody()));
            }
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        List<Envelope> results = _task.window();
        if (results != null) {
            for (Envelope result : results) {
                send(result, collector);
            }
        }
    }

    @Override
    public void close() throws Exception {
        // Guard against the edge case where close is called before init().
        if (_task != null) {
            _task.shutdown();
        }
    }

}
