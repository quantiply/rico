package com.quantiply.avro;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface AvroToJSONCustomizer {

    void customize(ObjectMapper objectMapper);

}
