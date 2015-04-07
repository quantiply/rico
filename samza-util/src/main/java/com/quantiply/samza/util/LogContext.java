package com.quantiply.samza.util;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.slf4j.MDC;

public class LogContext {
    private TaskContext context;

    public LogContext(TaskContext context) {
        this.context = context;
    }

    public void setMDC(IncomingMessageEnvelope envelope) {
        SystemStreamPartition ssp = envelope.getSystemStreamPartition();
        MDC.put("task", context.getTaskName().getTaskName());
        MDC.put("msg-offset", envelope.getOffset());
        MDC.put("msg-ssp", String.format("%s,%s,%s", ssp.getSystem(), ssp.getStream(), ssp.getPartition().getPartitionId()));
    }

    public void clearMDC() {
        MDC.remove("task");
        MDC.remove("msg-offset");
        MDC.remove("msg-ssp");
    }

}
