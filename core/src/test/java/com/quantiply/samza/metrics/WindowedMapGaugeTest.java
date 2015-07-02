package com.quantiply.samza.metrics;

import static org.mockito.Mockito.*;

import org.apache.samza.util.Clock;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class WindowedMapGaugeTest {
    private final Clock clock = mock(Clock.class);

    @Test
    public void testBuckets() throws Exception {
        WindowedMapGauge<Long> gauge = new WindowedMapGauge<>("wtf", 60000L, Long::max);
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L), gauge.getWindowStartTimes(0L));
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L), gauge.getWindowStartTimes(1L));
        assertEquals(new WindowedMapGauge.Windows(0L, -60000L) , gauge.getWindowStartTimes(59999L));
        assertEquals(new WindowedMapGauge.Windows(60000L, 0L), gauge.getWindowStartTimes(60000L));
        assertEquals(new WindowedMapGauge.Windows(300000L, 240000L), gauge.getWindowStartTimes(300001L));
    }

    @Test
    public void testUpdate() throws Exception {
        final long windowMs = 60000L;
        when(clock.currentTimeMillis()).thenReturn(0L);

        WindowedMapGauge<Long> gauge = new WindowedMapGauge<>("wtf", windowMs, Long::max, clock);
        gauge.update("key1", 5L);
        gauge.update("key1", 6L);
        gauge.update("key1", 4L);
        //Previous is still empty
        assertEquals(0, ((Map)gauge.getValue().get("data")).size());

        //Jump one window ahead
        when(clock.currentTimeMillis()).thenReturn(windowMs);
        //State not updated yet
        assertEquals(0, ((Map) gauge.getValue().get("data")).size());
        gauge.update("key1", 20L);
        System.out.println(gauge.getValue());
        assertEquals(6L, ((Map<String, Long>) gauge.getValue().get("data")).get("key1").longValue());

        //Jump another window ahead
        when(clock.currentTimeMillis()).thenReturn(windowMs*2);
        assertEquals(6L, ((Map<String, Long>) gauge.getValue().get("data")).get("key1").longValue());
        gauge.update("key1", 0L);
        assertEquals(20L, ((Map<String, Long>) gauge.getValue().get("data")).get("key1").longValue());

        //Jump two windows ahead
        when(clock.currentTimeMillis()).thenReturn(windowMs*4);
        assertEquals(0, ((Map) gauge.getValue().get("data")).size());
    }
}