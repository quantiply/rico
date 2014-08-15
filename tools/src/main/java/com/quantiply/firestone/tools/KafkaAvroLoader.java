package com.quantiply.firestone.tools;

import java.util.Properties;
import static java.nio.charset.StandardCharsets.*;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaAvroLoader {
  
  public static void main(String[] args) throws Exception {
    Namespace ns = parseCmdLineArgs(args);
    
    String bucket = ns.getString("bucket");
    String stream = ns.getString("stream");
    String file = ns.getString("file");
    
    String topic = String.format("pub.%s.%s", bucket, stream);
    
    //Read the schema from the file
    //Register the schema if necessary
    //Iterate through the file
    // for each record
    //   add to queue for Kafka
    //     add metadata and wrappers
    
    //Enqueue a message
    //new ProducerRecord("the-topic", "key, "value")
    //enqueue(producer, headers, body)
    
    Properties props = new Properties();
    KafkaProducer producer = new KafkaProducer(props);
    ProducerRecord record = new ProducerRecord(topic, "Hello".getBytes(UTF_8));
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