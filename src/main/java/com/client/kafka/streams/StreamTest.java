package com.client.kafka.streams;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;


public class StreamTest {

	public StreamTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsConfig config = new StreamsConfig(props);
		KStreamBuilder builder = new KStreamBuilder();
		builder.stream("my-input-topic").mapValues(value->value.toString()).to("my-output-topic");;
//		builder.stream("my-input-topic").mapValues(value -> value.length().toString()).to("my-output-topic");
		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();
	}
}
