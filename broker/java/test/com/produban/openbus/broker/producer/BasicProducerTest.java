package com.produban.openbus.broker.producer;

import static org.junit.Assert.*;
import kafka.message.MessageAndMetadata;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.produban.openbus.broker.consumer.BasicConsumer;

public class BasicProducerTest {

	static BasicConsumer consumer;
	static KafkaLocal kafka;

	@BeforeClass
	public static void startKafka(){		
		try {
			kafka = new KafkaLocal();
			Thread.sleep(5000);
		} catch (Exception e){
			fail("Error instantiating local Kafka broker");
			System.out.println(e.getMessage());
		}
		consumer = new BasicConsumer("test", kafka.getKafkaProperties().getProperty("zookeeper.connect"), "test_consumer_group");
	}

	@Test
	public void sendMessageToNewTopic() {
		String topic = "test";
		BasicProducer producer = new BasicProducer("localhost:9092", true);
		//produce some messages
		producer.sendMessage(topic, "1", "first message");
		producer.sendMessage(topic, "2", "second message");

		MessageAndMetadata<byte[], byte[]> firstMessage = consumer.consumeOne();
		assertEquals("first message", firstMessage.message());
		
		MessageAndMetadata<byte[], byte[]> secondMessage = consumer.consumeOne();
		assertEquals("second message", secondMessage.message());

	}

	@Test
	public void sendRandomLogEvents() {
		fail("ouch!");
	}

	@AfterClass
	public static void stopKafka(){

	}

}
