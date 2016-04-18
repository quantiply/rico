package com.quantiply.rico.samza.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.task.BaseTask;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;

/***
 * A generic task for pushing data to druid via tranquility.
 */
public class DruidHTTPPushTask extends BaseTask implements WindowableTask {

    private SystemStream systemStreamOut;
    private Queue<Object> buffer = new LinkedList<>();
    private int bufferSize = 0;
    final ObjectMapper mapper = new ObjectMapper();
    final CloseableHttpClient client = HttpClients.createDefault();
    private String tranquilityHTTPURL = null;
    private String dataSource = null;


    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        systemStreamOut = new SystemStream("druid", "dummy");
        bufferSize = config.getInt("rico.druid.push.http.buffer.size", 1000);
        tranquilityHTTPURL = config.get("rico.druid.push.http.url");
        dataSource = config.get("rico.druid.datasource");
        registerDefaultHandler(this::processMsg);
    }

    private void sendAndClearBuffer() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            mapper.writeValue(out, buffer);
            final byte[] data = out.toByteArray();
            System.out.println(new String(data));

            HttpPost httpPost = new HttpPost(tranquilityHTTPURL + dataSource);
            StringEntity entity = new StringEntity(new String(data));
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");

            CloseableHttpResponse response = client.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new RuntimeException("Server error. Returned status code = " + statusCode);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        buffer.add(envelope.getMessage());
        if (buffer.size() >= bufferSize) {
            sendAndClearBuffer();
        }
    }

    /* For testing in the IDE.*/
    public static void main(String... args) {
        String rootDir = Paths.get(".").toAbsolutePath().normalize().toString();
        String[] params = {
                "--config-factory",
                "org.apache.samza.config.factories.PropertiesConfigFactory",
                "--config-path",
                String.format("file://%s/samza-druid/src/main/config/%s.properties", rootDir, "tranquility")
        };
        JobRunner.main(params);

    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        sendAndClearBuffer();
    }

    @Override
    protected void _close() throws Exception {
        client.close();
    }
}
