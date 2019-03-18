package com.example.kafkatwitterconsumer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

public class TweetDeserializationSchema implements DeserializationSchema<Tweet> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Tweet deserialize(byte[] bytes) throws IOException {
        System.out.println("check here : " + bytes);
        try {
            String myString = new String(bytes);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return objectMapper.readValue(bytes, Tweet.class);
    }

    @Override
    public boolean isEndOfStream(Tweet tweet) {
        return false;
    }

    @Override
    public TypeInformation<Tweet> getProducedType() {
        return TypeInformation.of(Tweet.class);
    }
}
