package com.example.kafkatwitterconsumer;


import com.example.kafkatwitterconsumer.model.TweetAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class Pipeline {

    public static void invoke() throws Exception {

        //create the environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        log.info("Created environment");

        //create a consumer
        FlinkKafkaConsumer<String> consumer = createStringConsumerForTopic("twitterTopic0317191009");
        consumer.setStartFromEarliest();
        log.info("Created consumer");

        //create a producer
        FlinkKafkaProducer<TweetAccumulator> flinkCapitalizedProducer = createKafkaProducer("twitterTopic0317191009");
        log.info("Created producer");

        //add your consumer as a source for your environment and get an inputStream
        DataStream<String> inputStream = environment.addSource(consumer);
        log.info("Created inputStream");


//        log.info("Writing to topic capitalizedTweets");
//        inputStream
//                .keyBy(schema -> schema.getId())
//                .timeWindow(Time.seconds(10))
//                .apply(new WindowFunction<Tweet, TweetAccumulator, Long, TimeWindow>() {
//
//                    @Override
//                    public void apply(Long tweetId, TimeWindow timeWindow, Iterable<Tweet> iterable, Collector<TweetAccumulator> collector) throws Exception {
//                        int count = ((Collection<?>) iterable).size();
//                        System.out.println("Count : " + count);
//                        collector.collect(new TweetAccumulator(tweetId, timeWindow.getEnd(), count));
//                    }
//                });
        //.addSink(flinkCapitalizedProducer);

        DataStream<Status> statusStream = inputStream.map(str -> {
            Status status = null;
            try {
                status = TwitterObjectFactory.createStatus(str);
            } catch (TwitterException e) {
                e.printStackTrace();
            }
            return status;
        });

        //inputStream.print();
        statusStream.print();

        environment.execute();

    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic) {

        Properties properties = new Properties();

        //pull from config file
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        return consumer;

    }

    public static FlinkKafkaProducer<TweetAccumulator> createKafkaProducer(String topic) {
        //return new FlinkKafkaProducer<>("localhost:9092", topic, new SimpleStringSchema());
        return null;
    }

    public static String generateScore() {
        //this will be replace by the sentiment analysis call...
        UUID uuid = UUID.randomUUID();
        return uuid.toString() + "," + (Math.random() * 50 + 1);
    }

}
