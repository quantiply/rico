/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza;

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.slf4j.MDC;

public class TaskInfo {
    private final static String JAVA_OPTS_CONTAINER_NAME = "samza.container.name";
    private TaskContext context;
    private Config config;
    private String containerName;

    public TaskInfo(Config config, TaskContext context) {
        this.config = config;
        this.context = context;
        containerName = System.getProperty(JAVA_OPTS_CONTAINER_NAME);
    }

    public TaskContext getContext() {
        return context;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getTaskName() {
        return context.getTaskName().getTaskName();
    }

    public String getJobName() {
        return config.get(JobConfig.JOB_NAME(), null);
    }

    public String getJobId() {
        return config.get(JobConfig.JOB_ID(), null);
    }

    public void setMDC(IncomingMessageEnvelope envelope) {
        SystemStreamPartition ssp = envelope.getSystemStreamPartition();
        MDC.put("task", getTaskName());
        MDC.put("msg-offset", envelope.getOffset());
        MDC.put("msg-ssp", String.format("%s,%s,%s", ssp.getSystem(), ssp.getStream(), ssp.getPartition().getPartitionId()));
    }

    public void clearMDC() {
        MDC.remove("task");
        MDC.remove("msg-offset");
        MDC.remove("msg-ssp");
    }

}
