//package com.quantiply.druid;
//
//import com.quantiply.api.Configuration;
//import com.quantiply.api.Processor;
//import com.quantiply.api.Context;
//import org.apache.log4j.Logger;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//public class TranquilityProcessor implements Processor<Map<String, Object> > {
//
//    private final static Logger LOG = Logger.getLogger(TranquilityProcessor.class);
//
//    private Tranquility _tq;
//
//    @Override
//    public void init(Configuration configuration, Context context) throws Exception {
//        _tq = new Tranquility(configuration);
//        LOG.info("Tranquility config : " + configuration);
//    }
//
//    @Override
//    public List<Map<String, Object> > process(List<Map<String, Object>> events) throws Exception {
//        // Strip away the header.
//        List <Map<String, Object> > cleanEvents = new ArrayList<>();
//        events.forEach(event -> cleanEvents.add((Map<String, Object>) event.get("body")));
//
//        _tq.write(cleanEvents);
//
//        // TODO: Return errors (if possible).
//        return null;
//    }
//
//    @Override
//    public List window() throws Exception {
//        return null;
//    }
//
//    @Override
//    public void shutdown() throws Exception {
//        _tq.close();
//    }
//}
