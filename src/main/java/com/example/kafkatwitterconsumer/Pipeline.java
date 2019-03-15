package com.example.kafkatwitterconsumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Slf4j
public class Pipeline {

    public static void invoke() throws Exception {
      //create the environment
      StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
      log.info("Created environment");

      //create a consumer
      FlinkKafkaConsumer<String> consumer = createStringConsumerForTopic();
      consumer.setStartFromEarliest();
      log.info("Created consumer");

      //create a producer
      FlinkKafkaProducer<String> flinkCapitalizedProducer = createStringProducer();
      log.info("Created producer");

      //add your consumer as a source for your environment and get an inputStream
      DataStream<String> inputStream = environment.addSource(consumer);
      log.info("Created inputStream");


      log.info("Writing to topic capitalizedTweets");
      inputStream.map(s -> {
          log.info("Received a tweet...");
          return generateScore();
      }).addSink(flinkCapitalizedProducer);

      environment.execute();

    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic() {

        Properties properties = new Properties();

        //pull from config file
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("twitterTopic", new SimpleStringSchema(), properties);

        return consumer;

    }

    public static FlinkKafkaProducer<String> createStringProducer() {
        return new FlinkKafkaProducer<>("localhost:9092", "capitalizedTweets", new SimpleStringSchema());
    }

    public static String generateScore() {
        //this will be replace by the sentiment analysis call...
        UUID uuid = UUID.randomUUID();
        return uuid.toString() + "," + (Math.random() * 50 + 1);
    }
}
