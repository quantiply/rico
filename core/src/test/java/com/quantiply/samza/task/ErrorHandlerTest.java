package com.quantiply.samza.task;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorHandlerTest {

    private final Clock clock = mock(Clock.class);

    @Test
    public void testNotTooManyErrors() throws Exception {
        ErrorHandler errHandler = getErrorHandler();
        BaseTask.StreamMetrics metrics = new BaseTask.StreamMetrics(new Meter(clock), new Meter(clock));

        when(clock.getTick()).thenReturn(0L);
        metrics.processed.mark(910L);
        metrics.dropped.mark(90L);

        when(clock.getTick()).thenReturn(TimeUnit.SECONDS.toNanos(10));
        assertTrue(metrics.processed.getOneMinuteRate() > 0);
        assertTrue(metrics.dropped.getOneMinuteRate() > 0);
        assertFalse(errHandler.hasTooManyErrors(metrics));
    }

    @Test
    public void testTooManyErrors() throws Exception {
        ErrorHandler errHandler = getErrorHandler();
        BaseTask.StreamMetrics metrics = new BaseTask.StreamMetrics(new Meter(clock), new Meter(clock));

        assertEquals(0.1, errHandler.getDropMaxRatio(), 0.00000001);
        assertFalse(errHandler.hasTooManyErrors(metrics));

        when(clock.getTick()).thenReturn(0L);
        metrics.processed.mark(910L);
        metrics.dropped.mark(900L);
        assertFalse(metrics.processed.getOneMinuteRate() > 0);
        assertFalse(metrics.dropped.getOneMinuteRate() > 0);

        when(clock.getTick()).thenReturn(TimeUnit.SECONDS.toNanos(10));
        assertTrue(metrics.processed.getOneMinuteRate() > 0);
        assertTrue(metrics.dropped.getOneMinuteRate() > 0);
        assertTrue(errHandler.hasTooManyErrors(metrics));
    }

    private ErrorHandler getErrorHandler() {
        HashMap<String, String> cfgMap = new HashMap<>();
        cfgMap.put("rico.drop.max.ratio", "0.1");
        Config cfg = new MapConfig(cfgMap);
        ErrorHandler errHandler = new ErrorHandler(cfg, null);
        errHandler.start();
        return errHandler;
    }
}