package com.quantiply.samza.task;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import com.quantiply.samza.util.LogContext;
import org.apache.avro.Schema;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import javax.naming.ConfigurationException;

public class AvroToJSONTask implements InitableTask, StreamTask {
    private AvroSerde avroSerde;
    private LogContext logContext;
    private SystemStream outStream;
    private final ObjectMapper objMapper;

    @JsonIgnoreType
    abstract class IgnoreTypeMixIn { }

    {
        objMapper = new ObjectMapper();
        //Ignore schemas
        objMapper.addMixInAnnotations(Schema.class, IgnoreTypeMixIn.class);
        //Use only public getters
        objMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        objMapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
        //Use lower case with underscores for names
        objMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        logContext = new LogContext(context);
        avroSerde = new AvroSerdeFactory().getSerde("avro", config);
        String outTopic = config.get("topics.out");
        if (outTopic == null) {
            throw new ConfigurationException("Missing property for output topic: topics.out");
        }
        outStream = new SystemStream("kafka", outTopic);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        logContext.setMDC(envelope);

        Object inMsg = avroSerde.fromBytes((byte[]) envelope.getMessage());
        byte[] outMsg = objMapper.writeValueAsBytes(inMsg);
        collector.send(new OutgoingMessageEnvelope(outStream, outMsg));

        logContext.clearMDC();
    }
}
