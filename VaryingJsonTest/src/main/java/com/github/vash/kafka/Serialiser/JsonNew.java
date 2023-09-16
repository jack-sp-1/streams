package com.github.vash.kafka.Serialiser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class JsonNew implements Serializer<Object> {

    @Override
    public void configure(Map configs, boolean isKey) {
        //Serializer.super.configure(configs, isKey);

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        //return new byte[0];
        byte[] output = null;
        final ObjectMapper objectMapper = new ObjectMapper();

        try {
            output = objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return output;

    }


    @Override
    public void close() {
        //Serializer.super.close();
    }
}
