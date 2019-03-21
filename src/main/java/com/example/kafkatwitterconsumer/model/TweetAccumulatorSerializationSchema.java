package com.example.kafkatwitterconsumer.model;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;


public class TweetAccumulatorSerializationSchema implements SerializationSchema<TweetAccumulator> {

     static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(TweetAccumulator tweetAccumulator) {
        try {
            return objectMapper.writeValueAsString(tweetAccumulator).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }
}
