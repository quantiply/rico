package com.rico.samza;

import com.metamx.common.logger.Logger;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.samza.BeamFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;

import java.io.InputStream;
import java.util.Map;

public class DruidLoader implements BeamFactory
{

    private static final Logger log = new Logger(DruidLoader.class);


    @Override
    public Beam makeBeam(SystemStream stream, Config config)
    {
//        final String zkConnect = "localhost";
//        final String dataSource = stream.getStream();
//
//        final List<String> dimensions = ImmutableList.of("column");
//        final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
//                new CountAggregatorFactory("cnt")
//        );
//
//        // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
//        // Objects, so you'll generally have to cast them here.
//        final Timestamper<Object> timestamper = new Timestamper<Object>()
//        {
//            @Override
//            public DateTime timestamp(Object obj)
//            {
//                final Map<String, Object> theMap = (Map<String, Object>) obj;
//                return new DateTime(theMap.get("timestamp"));
//            }
//        };
//
//        final CuratorFramework curator = CuratorFrameworkFactory.builder()
//                .connectString(zkConnect)
//                .retryPolicy(new ExponentialBackoffRetry(500, 15, 10000))
//                .build();
//        curator.start();
//
////        return DruidBeams
////                .builder(timestamper)
////                .curator(curator)
////                .discoveryPath("/druid/discovery")
////                .location(DruidLocation.create("druid/overlord", "druid:firehose:%s", dataSource))
////                .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularity.MINUTE))
////                .tuning(
////                        ClusteredBeamTuning.builder()
////                                .segmentGranularity(Granularity.HOUR)
////                                .windowPeriod(new Period("PT10M"))
////                                .build()
////                )
////                .buildBeam();

        final InputStream configStream = DruidLoader.class.getClassLoader().getResourceAsStream("server.json");
        final TranquilityConfig<PropertiesBasedConfig> cfg = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = cfg.getDataSource("pageviews2");
        Beam<Map<String, Object>> beam = DruidBeams.fromConfig(wikipediaConfig).buildBeam();
        return beam;
    }
}