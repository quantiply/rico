package com.quantiply.samza.serde;

import com.quantiply.test.SimpleUser;
import com.quantiply.test.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class AvroSerdeTest {

  @Test
  public void testSerde() throws IOException {
    Properties props = new Properties();
    props.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    AvroSerde avroSerde = new AvroSerde(new MockSchemaRegistryClient(), new VerifiableProperties(props));
    User user = User.newBuilder()
        .setName("Cornhoolio")
        .setAge(12)
        .build();

    byte[] bytes = avroSerde.toBytes(user);

    User userRead = (User) avroSerde.fromBytes(bytes);
    assertEquals(user, userRead);

    SimpleUser simpleUser = (SimpleUser) avroSerde.fromBytes(bytes, SimpleUser.getClassSchema());
    assertEquals("Cornhoolio", simpleUser.getName().toString());
  }

  @Test
  public void testSerdeWithGenericData() throws IOException {
    Properties props = new Properties();
    props.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
    AvroSerde avroSerde = new AvroSerde(new MockSchemaRegistryClient(), new VerifiableProperties(props));
    User user = User.newBuilder()
        .setName("Cornhoolio")
        .setAge(12)
        .build();

    byte[] bytes = avroSerde.toBytes(user);

    GenericData.Record userRead = (GenericData.Record) avroSerde.fromBytes(bytes);
    assertNotNull(userRead.get("age"));

    GenericData.Record simpleUser = (GenericData.Record) avroSerde.fromBytes(bytes, SimpleUser.getClassSchema());
    //Projection masked the age field
    assertNull(simpleUser.get("age"));
  }
}
