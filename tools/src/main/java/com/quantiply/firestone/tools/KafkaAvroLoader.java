package com.quantiply.firestone.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.*;

import org.apache.log4j.BasicConfigurator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.ImmutableMap;
import com.quantiply.schema.WrappedMsg;

public class KafkaAvroLoader {
  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Namespace ns = parseCmdLineArgs(args);
    
    String bucket = ns.getString("bucket");
    String stream = ns.getString("stream");
    String fileName = ns.getString("file");
    
    String topic = String.format("pub.%s.%s", bucket, stream);
    
    //Read the schema from the file
    //Register the schema if necessary
    //Iterate through the file
    // for each record
    //   add to queue for Kafka
    //     add metadata and wrappers
    File file = new File(fileName);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
    datumReader.setSchema(dataFileReader.getSchema());
    
    GenericRecord avroRecord = null;
    while (dataFileReader.hasNext()) {
        avroRecord = dataFileReader.next(avroRecord);
        System.out.println(avroRecord);
    }
    
    //Enqueue a message
    //new ProducerRecord("the-topic", "key, "value")
    //enqueue(producer, headers, body)
    
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    KafkaProducer producer = new KafkaProducer(props);
    Map<CharSequence, CharSequence> headers = ImmutableMap.of((CharSequence)"Content-Type", (CharSequence)"text/plain");
    //TODO - add magic byte
    WrappedMsg msg = WrappedMsg.newBuilder()
            .setHeaders(headers)
            .setBody(ByteBuffer.wrap("Hello".getBytes(UTF_8)))
            .build();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(WrappedMsg.getClassSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(msg, encoder);
    encoder.flush();
    ProducerRecord record = new ProducerRecord(topic, out.toByteArray());
    try {
        producer.send(record).get();
    }
    finally {
        producer.close();
    }
    
    System.out.println("topic " + topic + " File " + file);
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