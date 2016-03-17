package com.quantiply.rico.samza.system.druid;

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

public class JSONBeamFactory implements BeamFactory
{

    private static final Logger log = new Logger(JSONBeamFactory.class);
    public static String TRANQUILITY_CONFIG_FILE = "server.json";

    @Override
    public Beam makeBeam(SystemStream stream, Config config) {

        InputStream configStream = JSONBeamFactory.class.getClassLoader().getResourceAsStream(TRANQUILITY_CONFIG_FILE);
        TranquilityConfig<PropertiesBasedConfig> tranquilityConfig = TranquilityConfig.read(configStream);
        DataSourceConfig<PropertiesBasedConfig> dataSourceConfig = tranquilityConfig.getDataSource(config.get("druid.loader.datasource"));
        return DruidBeams.fromConfig(dataSourceConfig).buildBeam();
    }
}