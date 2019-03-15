package com.example.kafkatwitterconsumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SocketWindowWordCount {


    public static void main(String[] args) throws Exception {
        //the port to connect to
        final int port = 9000;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //get input data by connecting to the socket
        DataStream<String> stream = env.socketTextStream("localhost", port, "\n");

        //parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = stream
                        .flatMap(new FlatMapFunction<String, WordWithCount>() {
                            @Override
                            public void flatMap(String value, Collector<WordWithCount> out) {
                                for (String word : value.split("\\s")) {
                                    out.collect(new WordWithCount(word, 1L));
                                }
                            }
                        })
                        .keyBy("word")
                        .timeWindow(Time.seconds(10), Time.seconds(10))
                        .reduce(new ReduceFunction<WordWithCount>() {

                            @Override
                            public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                                return new WordWithCount(a.word, a.count + b.count);
                            }

                        });

        //print the results
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordWithCount {

        public String word;
        public long count;

    }
}
