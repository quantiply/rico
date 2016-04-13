package com.quantiply.rico.samza.system.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.samza.BeamFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class JSONBeamFactory implements BeamFactory
{

    private static final Logger log = new Logger(JSONBeamFactory.class);
    public static String TRANQUILITY_CONFIG_FILE = "server.json";

    @Override
    public Beam makeBeam(SystemStream stream, Config config) {

        String datasource = getOrDie("druid.datasource", config);
        String zkConnect = getOrDie("druid.zookeeper.connect", config);
        String discoveryPath = getOrDie("druid.discovery.curator.path", config);
        String overlordSelector = getOrDie("druid.selectors.indexing.serviceName", config);

        System.out.println("****************");
        System.out.println("****************" + datasource);
        System.out.println("****************" + discoveryPath);
        System.out.println("****************" + overlordSelector);


        InputStream configStream = JSONBeamFactory.class.getClassLoader().getResourceAsStream(TRANQUILITY_CONFIG_FILE);
        TranquilityConfig<PropertiesBasedConfig> tranquilityConfig = TranquilityConfig.read(configStream);
        DataSourceConfig<PropertiesBasedConfig> dataSourceConfig = tranquilityConfig.getDataSource(datasource);

        final CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(500, 15, 10000))
                .build();
        curator.start();

        return DruidBeams
                .fromConfig(dataSourceConfig)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(overlordSelector, "druid:firehose:%s", datasource))
                .buildBeam();
    }

    private String getOrDie(String key, Config config){
        if(config.containsKey(key)) {
            return config.get(key);
        } else {
            throw new IllegalArgumentException("Missing property [ " + key+ " ]");
        }
    }
}