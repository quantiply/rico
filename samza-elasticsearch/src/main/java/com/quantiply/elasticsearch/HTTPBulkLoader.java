package com.quantiply.elasticsearch;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import com.quantiply.samza.task.ESPushTaskConfig;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class HTTPBulkLoader {
  protected final Optional<Integer> maxActions;
  protected final JestClient client;
  protected final Consumer<BulkResult> afterFlush;
  protected final List<BulkableAction<DocumentResult>> actions;
  protected Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

  public HTTPBulkLoader(ESPushTaskConfig.ESClientConfig clientConfig, Consumer<BulkResult> afterFlush) {
    String elasticUrl = String.format("http://%s:%s", clientConfig.httpHost, clientConfig.httpPort);
    maxActions = clientConfig.flushMaxActions;
    JestClientFactory jestFactory = new JestClientFactory();
    jestFactory.setHttpClientConfig(new HttpClientConfig.Builder(elasticUrl).multiThreaded(true).build());
    client = jestFactory.getObject();
    this.afterFlush = afterFlush;
    actions = new ArrayList<>();
  }

  public void addAction(ESPushTaskConfig.ESIndexSpec spec, ActionRequestKey requestKey, Object source) throws IOException {
    BulkableAction<DocumentResult> action = null;
    if (requestKey.getAction().equals(Action.INDEX)) {
      Index.Builder builder = new Index.Builder(source)
          .id(requestKey.getId().toString())
          .index(getIndex(spec, requestKey))
          .type(spec.docType);
      if (requestKey.getVersionType() != null) {
        builder.setParameter(Parameters.VERSION_TYPE, requestKey.getVersionType().toString());
      }
      if (requestKey.getVersion() != null) {
        builder.setParameter(Parameters.VERSION, requestKey.getVersion());
      }
      action = builder.build();
    }
    actions.add(action);
    checkFlush();
  }

  public void onInterval() throws IOException {
    checkFlush();
  }

  public void flush() throws IOException {
    Bulk bulkRequest = new Bulk.Builder().addAction(actions).build();
    try {
      BulkResult bulkResult = client.execute(bulkRequest);

      //TODO - checkpoint Samza in the callback
      afterFlush.accept(bulkResult);
    }
    catch (Exception e) {
      logger.error("FUCK ME");
      throw e;
    }
    finally {
      actions.clear();
    }
  }

  protected boolean checkFlush() throws IOException {
    return false;
  }

  protected String getIndex(ESPushTaskConfig.ESIndexSpec spec, ActionRequestKey requestKey) {
    if (spec.indexNameDateFormat.isPresent()) {
      ZonedDateTime dateTime = Instant.ofEpochMilli(requestKey.getPartitionTsUnixMs()).atZone(spec.indexNameDateZone);
      //ES index names must be lowercase
      String dateStr = dateTime.format(DateTimeFormatter.ofPattern(spec.indexNameDateFormat.get())).toLowerCase();
      return spec.indexNamePrefix + dateStr;
    }
    return spec.indexNamePrefix;
  }

}
