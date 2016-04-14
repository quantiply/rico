/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza.task;

import com.quantiply.avro.AvroToJSONCustomizer;
import com.quantiply.avro.AvroToJSONCustomizerFactory;
import com.quantiply.avro.AvroToJson;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.metrics.EventStreamMetrics;
import com.quantiply.samza.metrics.EventStreamMetricsFactory;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/**

  Converts Avro to JSON

  This task expects:
  - the input messages to be Avro
  - to fetch the schema from Confluent schema registry
  - the input and output topics are configured to use ByteSerde
  - the output topic name to be specified with the "rico.streams.out" property
  - if you want lag metrics, the Avro message should have a "header" fields with "created" and "time" child fields.

  This task:
  - Preserves message keys
  - Preserves partitioning

  Caveats:
   - It currently assumes lowercase with underscore for naming JSON fields.  This could be configurable later

 */
public class AvroToJSONTask extends BaseTask {
    public final static String CFG_CUSTOMIZER_FACTORY = "rico.avro.to.json.customizer.factory";
    private AvroSerde avroSerde;
    private SystemStream outStream;
    private AvroToJson avroToJson;
    private int numOutPartitions;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        registerDefaultHandler(this::processMsg, new EventStreamMetricsFactory());
        avroSerde = new AvroSerdeFactory().getSerde("avro", config);
        outStream = getSystemStream("out");
        avroToJson = getAvroToJson(config);
        numOutPartitions = getNumPartitionsForSystemStream(outStream);
    }

    protected AvroToJson getAvroToJson(Config config) {
        String factoryClass = config.get(CFG_CUSTOMIZER_FACTORY);
        if (factoryClass == null) {
            return new AvroToJson();
        }
        try {
            Class<AvroToJSONCustomizerFactory> klass = (Class<AvroToJSONCustomizerFactory>)Class.forName(factoryClass.trim());
            return new AvroToJson(klass.newInstance().getCustomizer());
        } catch (ClassNotFoundException e) {
            throw new ConfigException("Customizer factory class not found: " + factoryClass);
        } catch (InstantiationException e) {
            throw new ConfigException("Could not instantiate customizer factory class: " + factoryClass);
        } catch (IllegalAccessException e) {
            throw new ConfigException("Could not access customizer factory class: " + factoryClass);
        }
    }

    protected void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, EventStreamMetrics metrics) throws Exception {
        SpecificRecord inMsg = (SpecificRecord) avroSerde.fromBytes((byte[]) envelope.getMessage());
        updateLagMetricsForCamusRecord(inMsg, System.currentTimeMillis(), metrics);
        byte[] outMsg = avroToJson.objectToJson(inMsg);
        OutgoingMessageEnvelope outEnv = new OutgoingMessageEnvelope(outStream, outMsg,
                    envelope.getSystemStreamPartition().getPartition().getPartitionId() % numOutPartitions,
                    envelope.getKey());
        collector.send(outEnv);
    }

}
