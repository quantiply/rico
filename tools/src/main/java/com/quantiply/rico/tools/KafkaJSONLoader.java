package com.quantiply.rico.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.quantiply.rico.common.codec.Headers;
import com.quantiply.rico.common.codec.StringEncoder;
import com.quantiply.rico.common.codec.StringMessage;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class KafkaJSONLoader {

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Namespace ns = parseCmdLineArgs(args);

        String tenant = ns.getString("tenant");
        String bucket = ns.getString("bucket");
        String stream = ns.getString("stream");
        String fileName = ns.getString("file");
        String timeField = ns.getString("timeField");
        String keyField = ns.getString("keyField");
        
        String topic = String.format("pub.%s.%s.%s", tenant, bucket, stream);
        
        System.out.println(ns.toString());
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        KafkaProducer producer = new KafkaProducer(props);
        
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                ProducerRecord record = getProducerRecord(topic, line, timeField, keyField);
                producer.send(record);
            }
        }
        finally {
            br.close();
            producer.close();
        }
    }
    
    private static ProducerRecord getProducerRecord(String topic, String jsonRec, String timeField, String keyField) throws IOException {
        String id = UUID.randomUUID().toString();
        JsonObject jObj = new JsonParser().parse(jsonRec).getAsJsonObject();
        DateTime occurred = ISODateTimeFormat.dateTime().parseDateTime(jObj.get(timeField).getAsString());
        Headers hdrs = new Headers(id, occurred);
        StringMessage msg = new StringMessage(jsonRec, hdrs);
        StringEncoder encoder = new StringEncoder();
        byte[] key = null;
        if (keyField != null) {
            key = jObj.get(keyField).getAsString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        return new ProducerRecord(topic, key, encoder.encode(msg));
    }

    private static Namespace parseCmdLineArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("KafkaJSONLoader")
                .defaultHelp(true)
                .description("Load line-based JSON file to Kafka");
        parser.addArgument("-t", "--tenant").required(true)
            .help("tenant");
        parser.addArgument("-b", "--bucket").required(true)
            .help("bucket");
        parser.addArgument("-s", "--stream").required(true)
            .help("stream");
        parser.addArgument("--timeField").required(false).setDefault("@timestamp")
            .help("timestamp field (ISO 1806 format)");
        parser.addArgument("-k", "--keyField").required(false)
            .help("partition key field");
        parser.addArgument("file")
            .help("JSON file");
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
