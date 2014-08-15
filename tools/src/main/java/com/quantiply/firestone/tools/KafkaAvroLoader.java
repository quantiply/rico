package com.quantiply.firestone.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class KafkaAvroLoader {
  
  public static void main(String[] args) {
    Namespace ns = parseCmdLineArgs(args);
    
    String bucket = ns.getString("bucket");
    String stream = ns.getString("stream");
    String file = ns.getString("file");
    
    
    System.out.println("Bucket " + bucket + " Stream " + stream + " File " + file);
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