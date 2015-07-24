package com.quantiply.samza.metrics;

import org.apache.samza.util.Clock;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopNWindowedMapGaugeTest {
    private final Clock clock = mock(Clock.class);

    @Test
    public void testTopN() throws Exception {
        final long windowMs = 60000L;
        when(clock.currentTimeMillis()).thenReturn(0L);

        WindowedMapGauge<Long> gauge = new TopNWindowedMapGauge<>("wtf", windowMs, Long::max, 3, clock);
        gauge.update("key1", 1L);
        gauge.update("key2", 2L);
        gauge.update("key3", 3L);
        gauge.update("key4", 4L);
        gauge.update("key5", 5L);

        //Previous is still empty
        assertEquals(0, ((Map) gauge.getValue().get("data")).size());
        //Jump one window ahead
        when(clock.currentTimeMillis()).thenReturn(windowMs);
        //Reporter is ahead by one
        assertEquals(0, ((Map) gauge.getValue().get("data")).size());
        //This will align the state
        gauge.update("key1", 20L);
        //Report is now aligned
        assertEquals(3, ((Map<String, Long>) gauge.getValue().get("data")).size());
        //Make sure that largest 3 were kept
        assertEquals(
                new HashSet<String>(Arrays.asList("key3", "key4", "key5")),
                ((Map<String, Long>) gauge.getValue().get("data")).keySet());
    }

}