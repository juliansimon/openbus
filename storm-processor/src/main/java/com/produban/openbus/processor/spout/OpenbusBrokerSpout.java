/*
* Copyright 2013 Produban
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.produban.openbus.processor.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.Partition;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.spout.IPartitionedTridentSpout;

/**
 * Kafka Broker Openbus
 */
public class OpenbusBrokerSpout {	
	private static Logger LOG = LoggerFactory.getLogger(OpenbusBrokerSpout.class);
	
	private final static String KAFKA_TOPIC = "jsonTopic";
	private final static String ZOOKEEPERS_HOST = "192.168.20.136:2181";
	private final static String ZOOKEEPERS_BROKER = "/brokers"; 
	
	
	private TridentKafkaConfig config = null;
	private ZkHosts zhost = null; 
	private IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> partitionedTridentSpout = null;


	public OpenbusBrokerSpout() {
    	zhost = new ZkHosts(ZOOKEEPERS_HOST, ZOOKEEPERS_BROKER);        
        config = new TridentKafkaConfig(zhost, KAFKA_TOPIC);      
        config.forceStartOffsetTime(-2);
	}
	
	public OpenbusBrokerSpout(String kafkaTopic) {    	
    	zhost = new ZkHosts(ZOOKEEPERS_HOST, ZOOKEEPERS_BROKER);    	
        config = new TridentKafkaConfig(zhost, kafkaTopic);                        
        config.forceStartOffsetTime(-2);
	}
	
	public IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> getPartitionedTridentSpout() {		
		partitionedTridentSpout = new TransactionalTridentKafkaSpout(config);
		
		return partitionedTridentSpout;
	}
}