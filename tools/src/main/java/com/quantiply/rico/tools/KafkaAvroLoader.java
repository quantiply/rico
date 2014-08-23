package com.quantiply.rico.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.BasicConfigurator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.io.BaseEncoding;
import com.quantiply.rico.common.codec.AvroEncoder;
import com.quantiply.rico.common.codec.AvroMessage;
import com.quantiply.rico.common.codec.Headers;

public class KafkaAvroLoader {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Namespace ns = parseCmdLineArgs(args);

        String bucket = ns.getString("bucket");
        String stream = ns.getString("stream");
        String fileName = ns.getString("file");
        
        String topic = String.format("pub.%s.%s", bucket, stream);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(fileName), datumReader);
        datumReader.setSchema(dataFileReader.getSchema());

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        KafkaProducer producer = new KafkaProducer(props);

        GenericRecord avroRecord = null;
        try {
            while (dataFileReader.hasNext()) {
                avroRecord = dataFileReader.next(avroRecord);
                send(producer, topic, avroRecord);
            }
        }
        finally {
            producer.close();
            dataFileReader.close();
        }
    }

    private static RecordMetadata send(KafkaProducer producer, String topic, GenericRecord avroRecord)
            throws IOException, InterruptedException, ExecutionException, NoSuchAlgorithmException {
        
        GenericDatumWriter<GenericRecord> recWriter = new GenericDatumWriter<GenericRecord>(avroRecord.getSchema());
        ByteArrayOutputStream recOut = new ByteArrayOutputStream();
        Encoder recEncoder = EncoderFactory.get().binaryEncoder(recOut, null);
        recWriter.write(avroRecord, recEncoder);
        recEncoder.flush();
        
        //TODO - register the schema if necessary
        //  - could use Bloom filter for existence check
        //  - config option to skip registration
        
        /*
         * AvroProducer<GenericRecord> producer = new AvroProducer<GenericRecord>();
         * producer.send(bucket, stream, avroRecord, key);
         * with this in parent?
         * producer.send(RicoMessage m)
         * 
         * KafkaAvroEncoder.createRecord(bucket, stream, avroRecord, key, partition)
         *    - or RicoAvroEncoder??
         * 
         * Need a util that gives me a ProducerRecord from (bucket, stream, avroRecord, key, partition)
         * 
         * RicoMessage - NO - this is a ProducerRecord
         *  - topic
         *  - key
         *  - message
         * 
         *  SHOULD I ADD KEY TO THE AVROMESSAGE??? so I can have a util that returns a message
         *    - hmmm...add topic too??
         *    - DO WE EVER WANT THE KEY OR TOPIC ON THE CONSUMER???  Can we get it on the consumer?
         *    - What about a custom partitioner???
         * 
         *  - Get topic
         *  - Get schemaId and register if necessary
         *  
         *  RicoHelper.newAvroMessage(id(optional), schema, DateTime occurred)
         *  RicoHelper.getTopic(tenantId, bucket, stream)
         *  
         */
        
        byte[] fingerprint = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", avroRecord.getSchema());
        String schemaId = BaseEncoding.base64().encode(fingerprint);
        Headers hdrs = new Headers("fakeId", ISODateTimeFormat.dateTime().parseDateTime("2014-07-23T00:06:00.000Z"), schemaId, null);
        AvroMessage<GenericRecord> msg = new AvroMessage<GenericRecord>(avroRecord, hdrs);
        
        AvroEncoder<GenericRecord> encoder = new AvroEncoder<GenericRecord>();
        ProducerRecord record = new ProducerRecord(topic, encoder.encode(msg));
        return producer.send(record).get();
    }

    private static Namespace parseCmdLineArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("KafkaAvroLoader")
                .defaultHelp(true)
                .description("Load Avro file to Kafka");
        parser.addArgument("-b", "--bucket").required(true)
        .help("bucket");
        parser.addArgument("-s", "--stream").required(true)
        .help("stream");
        parser.addArgument("file")
        .help("Avro file");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        }
        catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return ns;
    }

}