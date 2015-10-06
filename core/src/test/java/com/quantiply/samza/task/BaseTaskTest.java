package com.quantiply.samza.task;

import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseTaskTest {

    class MyTask extends BaseTask {
        @Override
        protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        }
    }

    @Test
    public void getMessageIdFromSource() {
        MyTask myTask = new MyTask();
        String offset = "12345";
        SystemStreamPartition ssp = new SystemStreamPartition("fakeSystem", "fakeStream", new Partition(56789));
        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(ssp, offset, null, null);
        assertEquals("fakeStream-56789-12345", myTask.getMessageIdFromSource(envelope));
    }

}