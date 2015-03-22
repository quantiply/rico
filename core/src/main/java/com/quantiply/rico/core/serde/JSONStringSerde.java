package com.quantiply.rico.core.serde;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.serde.StringSerde;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rhoover on 3/21/15.
 */
public class JSONStringSerde implements StringSerde<Object> {
    private final static String FIELD_NAME_HEADER = "headers";
    private final static String FIELD_NAME_BODY = "body";
    private final ObjectMapper _mapper;

    {
        _mapper = new ObjectMapper(new JsonFactory());
    }

    @Override
    public void init(Configuration cfg) {}

    @Override
    public Envelope fromString(String msg) throws Exception {
        TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<HashMap<String,Object>>() {};

        Map<String,Object> json = _mapper.readValue(msg, typeRef);
        Envelope<Object> envelope = new Envelope<>();

        if(json.containsKey(FIELD_NAME_HEADER)) {
            envelope.setHeaders((Map<String, String>) json.get(FIELD_NAME_HEADER));
        }

        if(json.containsKey(FIELD_NAME_BODY)){
            envelope.setPayload(json.get(FIELD_NAME_BODY));
        } else {
            envelope.setPayload(json);
        }
        return envelope;
    }

    @Override
    public void writeTo(Envelope<Object> envelope, java.io.StringWriter writer) throws Exception {
        Map<String, Object> msg = new HashMap<>();
        msg.put(FIELD_NAME_HEADER, envelope.getHeaders());
        msg.put(FIELD_NAME_BODY, envelope.getPayload());
        _mapper.writeValue(writer, msg);
    }
}
