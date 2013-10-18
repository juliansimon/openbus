package com.produban.openbus.broker.producer;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;

public class KafkaLocal {

	public KafkaServerStartable kafka;
	public ZooKeeperLocal zookeeper;
	public Properties kafkaProperties;
	
	
	public KafkaLocal() throws IOException, InterruptedException{
		
		//start local zookeeper
		System.out.println("starting local zookeeper...");
		zookeeper = new ZooKeeperLocal();
		System.out.println("done");
		
		//start local kafka broker
		kafkaProperties = new Properties();
		kafkaProperties.load(getClass().getResourceAsStream("kafkalocal.properties"));
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
		
		kafka = new KafkaServerStartable(kafkaConfig);
		System.out.println("starting local kafka broker...");
		kafka.startup();
		System.out.println("done");
	}
	
	
	public void stop(){
		//stop kafka broker
		System.out.println("stopping kafka...");
		kafka.shutdown();
		System.out.println("done");
	}
	
	
	public Properties getKafkaProperties() {
		return kafkaProperties;
	}

	
	public void setKafkaProperties(Properties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}
	
	

}
