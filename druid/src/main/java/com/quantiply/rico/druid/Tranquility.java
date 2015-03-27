package com.quantiply.rico.druid;

import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.typeclass.Timestamper;
import com.quantiply.rico.Configuration;
import com.twitter.finagle.Service;
import com.twitter.util.Await;
import com.twitter.util.Future;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Tranquility {

    private final static Logger LOG = Logger.getLogger(Tranquility.class);


    final private Service<List<Map<String, Object>>, Integer> druidService;
    final private CuratorFramework curator;
    final private Configuration _cfg;

    private int _totalEventsProcessed = 0;

    public Tranquility(Configuration cfg) {
        _cfg = cfg;

        List<String> dimensions = (List<String>) cfg.get("dimensions");
        final List<AggregatorFactory> aggregators = getAggregations((List<Map<String, Object>>) cfg.get("aggregations"));

        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        final Timestamper<Map<String, Object>> timeStamper = new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
//                return new DateTime(theMap.get("timestamp")).withZone(DateTimeZone.forID("UTC"));
                return new DateTime(theMap.get(_cfg.getString("timestampColumn")));
            }
        };

        // Tranquility uses ZooKeeper (through Curator) for coordination.
        curator = CuratorFrameworkFactory
                .builder()
                .connectString(cfg.getString("zkConnectString"))
                // TODO: Make this configurable.
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();

        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        final TimestampSpec timestampSpec = new TimestampSpec(cfg.getString("timestampColumn"), cfg.getString("timestampFormat"));

        // Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
        // done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
        // In this case, we won't provide one, so we're just using Jackson.
        druidService = DruidBeams
                .builder(timeStamper)
                .curator(curator)
                .discoveryPath(cfg.getString("discoveryPath"))
                .location(
                        DruidLocation.create(
                                cfg.getString("indexingService"),
                                cfg.getString("firehosePattern"),
                                cfg.getString("dataSource")
                        )
                )
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators,
                        // TODO: Make this configurable.
                        QueryGranularity.MINUTE))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                // TODO: Make this configurable.
                                .segmentGranularity(Granularity.HOUR)
                                .windowPeriod(new Period(cfg.getString("windowPeriod")))
                                .partitions(cfg.getInt("partitions"))
                                .replicants(cfg.getInt("replicas"))
                                .build()
                )
                .buildJavaService();

        LOG.info("Is com.quantiply.druid available ? " + druidService.isAvailable());
    }

    public int write(List<Map<String, Object>> events) throws Exception {

        // Send events to Druid:
        LOG.debug("Sending data (" + events.size() + " events).");
        final Future<Integer> numSentFuture = druidService.apply(events);

        // Wait for confirmation
        final Integer numSent = Await.result(numSentFuture);

        _totalEventsProcessed = _totalEventsProcessed + numSent;
        LOG.info("Batch (received="+ events.size()  +" events, processed="+ numSent +" events). Total events processed=" + _totalEventsProcessed);

        //TODO : Is there a way to find out if the objects sent were corrupt ala ES.
        // (Apart from Druid metrics that is)

        // From code : Futures will be the number of events pushed, or an exception. Zero events pushed means we gave up on the task.
        // https://github.com/metamx/tranquility/blob/f969acde03946d93b63384aab24c94afc225e08a/src/main/scala/com/metamx/tranquility/com.quantiply.druid/DruidBeam.scala#L94

        // Tranquility throws away data outside the window.
        // See : L 126, 188
        //     https://github.com/metamx/tranquility/blob/f969acde03946d93b63384aab24c94afc225e08a/src/main/scala/com/metamx/tranquility/beam/ClusteredBeam.scala
        //
        if (events.size() != numSent ){

            Map<String, Object> youngestEvent = events.get(events.size() - 1);
            DateTime latestTimeStamp = new DateTime(youngestEvent.get(_cfg.getString("timestampColumn")));
            Minutes timeDiff = Minutes.minutesBetween(latestTimeStamp, DateTime.now());

            LOG.warn("Dropping data" +
                            ". events/thrownAway = " + (events.size() - numSent) +
                            ", time diff="+ timeDiff.getMinutes() + "m" +
                            ", window period=" + _cfg.getString("windowPeriod") +
                            ", latest event=" + latestTimeStamp +
                            ", now=" + DateTime.now()
                        );
        }

        return numSent;
    }

    public void close() throws Exception {

        Await.result(druidService.close());
        curator.close();
        LOG.info("Tranquility task shutdown successful.");
    }

    private List<AggregatorFactory> getAggregations(List<Map<String, Object>> aggs) {

        List<AggregatorFactory> fac = new ArrayList<>();
        for (Map<String, Object> agg : aggs) {
            String name = (String) agg.get("name");
            String type = (String) agg.get("type");
            switch (type) {
                case "count":
                    fac.add(new CountAggregatorFactory(name));
                    break;
                case "doubleSum":
                    fac.add(new LongSumAggregatorFactory(name, (String) agg.get("fieldName")));
                    break;
                case "cardinality":
                    Boolean byRow =  (Boolean) agg.get("byRow");
                    List<String> fieldNames = (List<String>) agg.get("fieldNames");
                    fac.add(new CardinalityAggregatorFactory(name, fieldNames, byRow));
                    break;
                case "approxHistogram":
                    Integer resolution = agg.containsKey("resolution")? (Integer) agg.get("resolution"): null;
                    Integer numBuckets = agg.containsKey("numBuckets")? (Integer) agg.get("numBuckets"): null;

                    // All this typecasting is insane, need a better way to do this.
                    Float lowerLimit = agg.containsKey("lowerLimit")? ((Double) agg.get("lowerLimit")).floatValue(): null;
                    Float upperLimit = agg.containsKey("upperLimit")? ((Double) agg.get("upperLimit")).floatValue(): null;
                    fac.add(new ApproximateHistogramAggregatorFactory(name, (String) agg.get("fieldName"), resolution, numBuckets, lowerLimit, upperLimit));
                    break;
                default:
                    throw new RuntimeException(agg.get("type") + " not supported.");
            }
        }
        return fac;
    }


}
