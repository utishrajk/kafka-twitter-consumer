package com.example.kafkatwitterconsumer;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class MyWindowFunction implements WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow> {


    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String, Double>> input, Collector<Tuple2<String, Double>> out) throws Exception {

        double sum = 0L;
        int count = 0;

        for(Tuple2<String, Double> record: input) {
            sum += record.f1;
            count++;
        }

        Tuple2<String, Double> result = input.iterator().next();
        result.f1 = (sum/count);
        out.collect(result);

    }
}
