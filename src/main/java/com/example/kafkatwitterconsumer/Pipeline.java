package com.example.kafkatwitterconsumer;


import com.example.kafkatwitterconsumer.model.TweetAccumulator;
import com.example.kafkatwitterconsumer.model.TweetAccumulatorSerializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Collection;
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
        Time maxAllowedLateness = Time.seconds(5);
        DataStream<String> inputStream = environment
                .addSource(consumer);


        DataStream<TweetAccumulator> accumulatorDataStream = inputStream
                .map(str -> {
                    Status status = null;
                    try {
                        status = TwitterObjectFactory.createStatus(str);
                    } catch (TwitterException e) {
                        e.printStackTrace();
                    }
                    return status;
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Status>(maxAllowedLateness) {
                    @Override
                    public long extractTimestamp(Status status) {
                        return status.getCreatedAt().getTime();
                    }
                })
                .map(new MapFunction<Status, Integer>() {
                    @Override
                    public Integer map(Status status) throws Exception {
                        return 1;
                    }
                })
                .timeWindowAll(Time.seconds(10))
                .apply(new AllWindowFunction<Integer, TweetAccumulator, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<TweetAccumulator> collector) throws Exception {
                        int count = ((Collection<?>) iterable).size();
                        collector.collect(new TweetAccumulator(timeWindow.getEnd(), count));
                    }
                });


        //accumulatorDataStream.print();

        FlinkKafkaProducer<TweetAccumulator> tweetAccumulatorFlinkKafkaProducer = createKafkaProducer("tweetAccumulator");

        accumulatorDataStream.addSink(tweetAccumulatorFlinkKafkaProducer);

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
        return new FlinkKafkaProducer<>("localhost:9092", topic, new TweetAccumulatorSerializationSchema());
    }

    public static String generateScore() {
        //this will be replace by the sentiment analysis call...
        UUID uuid = UUID.randomUUID();
        return uuid.toString() + "," + (Math.random() * 50 + 1);
    }

}
