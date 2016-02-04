package com.quantiply.elasticsearch;

import com.google.gson.Gson;
import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.params.Parameters;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HTTPBulkLoaderTest {

  @Test
  public void testConvertToJestActionIndex() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.INDEX)
        .setId("myId")
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("index", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("{}", action.getData(new Gson()));
    assertEquals(0, action.getParameter(Parameters.VERSION).size());
    assertEquals(0, action.getParameter(Parameters.VERSION_TYPE).size());

    ActionRequestKey keyWithVersion = ActionRequestKey.newBuilder()
        .setAction(Action.INDEX)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> actionWithVersion = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(keyWithVersion, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("external", actionWithVersion.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, actionWithVersion.getParameter(Parameters.VERSION).toArray()[0]);
  }

  @Test
  public void testConvertToJestActionUpdate() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.UPDATE)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("update", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("external", action.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, action.getParameter(Parameters.VERSION).toArray()[0]);
    assertEquals("{}", action.getData(new Gson()));
  }

  @Test
  public void testConvertToJestActionDelete() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.DELETE)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, null));
    assertEquals("delete", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("external", action.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, action.getParameter(Parameters.VERSION).toArray()[0]);
    assertEquals(null, action.getData(new Gson()));
  }


  private HTTPBulkLoader getBulkLoader() {
    HTTPBulkLoader.Config config = new HTTPBulkLoader.Config("Test", 20, Optional.empty());
    JestClient client = mock(JestClient.class);
    return new HTTPBulkLoader(config, client, Optional.empty());
  }
}