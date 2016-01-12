package com.quantiply.samza.serde;

import com.quantiply.test.SimpleUser;
import com.quantiply.test.User;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class AvroSerdeTest {

  @Test
  public void testSerde() throws IOException {
    Properties specificDecoderProps = new Properties();
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    specificDecoderProps.setProperty(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    AvroSerde avroSerde = new AvroSerde(new MockSchemaRegistryClient(), new VerifiableProperties(specificDecoderProps));
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

}
