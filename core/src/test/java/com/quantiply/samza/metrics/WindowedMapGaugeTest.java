package com.quantiply.samza.metrics;

import javafx.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class WindowedMapGaugeTest {

    @Test
    public void testBuckets() throws Exception {
        WindowedMapGauge<Long> gauge = new WindowedMapGauge<Long>("wtf", 60000L, Long::max);
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L), gauge.getWindowStartTimes(0L));
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L), gauge.getWindowStartTimes(1L));
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L) , gauge.getWindowStartTimes(59999L));
        assertEquals(new WindowedMapGauge.Windows(60000L, 0L), gauge.getWindowStartTimes(60000L));
        assertEquals(new WindowedMapGauge.Windows(300000L, 240000L), gauge.getWindowStartTimes(300001L));
    }
}