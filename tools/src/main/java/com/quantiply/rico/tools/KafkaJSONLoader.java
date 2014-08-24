package com.quantiply.rico.tools;

import org.apache.log4j.BasicConfigurator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class KafkaJSONLoader {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Namespace ns = parseCmdLineArgs(args);

        String tenant = ns.getString("tenant");
        String bucket = ns.getString("bucket");
        String stream = ns.getString("stream");
        String fileName = ns.getString("file");
        
        String topic = String.format("pub.%s.%s.%s", tenant, bucket, stream);
        
        System.out.println(ns.toString());

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
        parser.addArgument("--timestamp").required(false).setDefault("@timestamp")
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
