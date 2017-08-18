package com.client.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import com.client.kafka.builder.ConsumerBuilder;
import com.client.kafka.builder.ProducerBuilder;

public class PGsqlTest {

	public PGsqlTest() {
		// TODO Auto-generated constructor stub
	}

	@Test
	public void flumeTest() {
		System.out.println("发送kafka........");
		Producer<String, String> producer3 = new ProducerBuilder().kafkaProducerBuilder();
		for (int i = 0; i < 100000; i++) {
			String json = "{\"file\":\"tk1\",\"size\":\"test\"}";
			producer3.send(new ProducerRecord<String, String>("testlog", "k-s", json));
		}
		producer3.close();
	}

	@Test
	public void consumerTest2() {
		System.out.println("接收kafka Group2........");
		KafkaConsumer<String, String> consumer = new ConsumerBuilder().groupId("test3").kafkaConsumerBuilder();
		consumer.subscribe(Arrays.asList("gamelog"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
																			// poll”进行阻塞，直到数据到达（并且设置等待时间的好处是可以积累消息，组成大数据块一并发送）。
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}
}
