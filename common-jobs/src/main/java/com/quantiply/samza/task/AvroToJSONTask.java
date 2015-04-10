package com.quantiply.samza.task;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import org.apache.avro.Schema;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/**

  Converts Avro to JSON

  This task expects:
  - the input topic key to be serialized according the Camus/Confluent Platform spec
  - the input and output topics are configured to use ByteSerde
  - the output topic is specified with the "streams.out" property

  Caveats:
   - It currently assumes lowercase with underscore for naming JSON fields.  This could be configurable later

 */
public class AvroToJSONTask extends BaseTask {
    private AvroSerde avroSerde;
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
    public void _init(Config config, TaskContext context) throws Exception {
        registerDefaultHandler(this::processMsg);
        avroSerde = new AvroSerdeFactory().getSerde("avro", config);
        outStream = getSystemStream("out");
    }

    public void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Object inMsg = avroSerde.fromBytes((byte[]) envelope.getMessage());
        byte[] outMsg = objMapper.writeValueAsBytes(inMsg);
        OutgoingMessageEnvelope outEnv;
        if (envelope.getKey() == null) {
            outEnv = new OutgoingMessageEnvelope(outStream, outMsg);
        }
        else {
            outEnv = new OutgoingMessageEnvelope(outStream, outMsg,
                    envelope.getSystemStreamPartition().getPartition().getPartitionId(),
                    envelope.getKey());
        }
        collector.send(outEnv);
    }
}
