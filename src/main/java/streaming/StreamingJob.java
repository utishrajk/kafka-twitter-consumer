package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("temp", new SimpleStringSchema(),
				properties);
		myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());


		DataStream<Tuple2<String, Double>> keyedStream = env.addSource(myConsumer).flatMap(new Splitter()).keyBy(0)
				.timeWindow(Time.seconds(30))
				.apply(new WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow>() {

					@Override
					public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String, Double>> input,
                                      Collector<Tuple2<String, Double>> out) throws Exception {
						double sum = 0L;
						int count = 0;
						for (Tuple2<String, Double> record : input) {
							sum += record.f1;
							count++;
						}

						Tuple2<String, Double> result = input.iterator().next();
						result.f1 = (sum/count);
						out.collect(result);

					}
				});

		//create a producer
		FlinkKafkaProducer<String> producer = createStringProducer();


		keyedStream
				.map(new MapFunction<Tuple2<String,Double>, String>() {
					@Override
					public String map(Tuple2<String, Double> item) throws Exception {
						System.out.println("-------------------->" + item.f1);
						return Double.toString(item.f1);
					}
				})
				.addSink(producer);

		//keyedStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static FlinkKafkaProducer<String> createStringProducer() {
		return new FlinkKafkaProducer<String>("localhost:9092", "averageTopic", new org.apache.flink.api.common.serialization.SimpleStringSchema());
	}

}
