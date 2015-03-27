package com.quantiply.rico.samza;

import com.codahale.metrics.*;
import org.apache.samza.metrics.Gauge;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by arrawatia on 3/26/15.
 */
public class MapGauge extends Gauge<Map<String, String>> {

    private Metric _metric;
    private String _name;

    public MapGauge(String name, Metric metric) {
        super(name, new HashMap<String, String>());
        _metric = metric;
        _name = name;
    }

    @Override
    public Map<String, String> getValue() {
//        System.out.println("Metric class -> " + _metric.getClass() + "  is Meter ? " + Meter.class.isInstance(_metric));
//        System.out.println("Value of metric " + _name + ": "+ toMap(_name, _metric));
        return toMap(_name, _metric);
    }

    private Map<String, String> toMap(String name, Metric metric) {
        if(Meter.class.isInstance(metric)){
            return meter(name, (Meter) metric);
        }
        else if(Histogram.class.isInstance(metric)){
            return histogram(name, (Histogram) metric);
        }
        else if(Counter.class.isInstance(metric)){
            return counter(name, (Counter) metric);
        }
        else if(Timer.class.isInstance(metric)){
            return timer(name, (Timer) metric);
        }
        else if(Gauge.class.isInstance(metric)){
            return gauge(name, (Gauge) metric);
        }

        return null;
    }

    public Map<String, String> meter(String name, Meter meter) {
        Map<String,String> data = new HashMap<>();
        data.put("name", name);
        data.put("count","" + meter.getCount());
        data.put("oneMinuteRate","" + meter.getOneMinuteRate());
        data.put("fiveMinuteRate","" + meter.getFiveMinuteRate());
        data.put("fifteenMinuteRate","" + meter.getFifteenMinuteRate());
        data.put("meanRate","" + meter.getMeanRate());
        return data;
    }

    private Map<String, String> counter(String name, Counter counter) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name);
        data.put("count", "" + counter.getCount());
        return data;

    }

    private Map<String, String> histogram(String name, Histogram histogram) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name);
        final Snapshot snapshot = histogram.getSnapshot();
        data.put("min","" + snapshot.getMin());
        data.put("max","" + snapshot.getMax());
        data.put("mean","" + snapshot.getMean());
        data.put("stdDev","" + snapshot.getStdDev());
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile", "" + snapshot.get999thPercentile());
        return data;

    }

    private Map<String, String> timer(String name, Timer timer) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name);
        final Snapshot snapshot = timer.getSnapshot();
        data.put("min","" + snapshot.getMin());
        data.put("max","" + snapshot.getMax());
        data.put("mean","" + snapshot.getMean());
        data.put("stdDev","" + snapshot.getStdDev());
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile", "" + snapshot.get999thPercentile());
        return data;

    }

    private Map<String, String> gauge(String name, Gauge<?> gauge){
        Map<String,String> data = new HashMap<String,String>();
        data.put("name", name);
        data.put("value", "" + gauge.getValue());
        return data;

    }

}
