package com.produban.openbus.broker.producer;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicProducerTest {

	static ConsumerConnector consumer;
	static KafkaLocal kafka;

	@BeforeClass
	public static void startKafka(){		
		try {
			kafka = new KafkaLocal();
		} catch (Exception e){
			fail("Error instantiating local Kafka broker");
			System.out.println(e.getMessage());
		}

		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());	
	}

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", kafka.getKafkaProperties().getProperty("zookeeper.connect"));
		props.put("group.id", "test_consumer_group");

		return new ConsumerConfig(props);
	}

	@Test
	public void sendMessageToNewTopic() {
		String topic = "test";
		BasicProducer producer = new BasicProducer("localhost:9092", true);
		//produce some messages
		producer.sendMessage(topic, "1", "first message");
		producer.sendMessage(topic, "2", "second message");

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, new Integer(1));
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
	    ConsumerIterator<byte[], byte[]> it = stream.iterator();
	    while(it.hasNext())
	    System.out.println(new String(it.next().message()));

	}

	@Test
	public void sendRandomLogEvents() {
		fail("ouch!");
	}

	@AfterClass
	public static void stopKafka(){

	}

}
