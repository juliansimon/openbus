package com.produban.openbus.processor.properties;

public interface Conf {

	public static final String HOST_OPENTSDB = "pivhdsne";
	public static final int PORT_OPENTSDB = 4242;
	public static final String SCHEMA_APACHE_LOGS = "apacheLog.avsc";
	
	public static final long TIME_PERIOD_MAX_RESPONSETIME = 5000; // Miliseconds
	public static final long TIME_PERIOD_REQUESTCOUNT = 5000; // Miliseconds
	
	public final static String KAFKA_TOPIC_SEND = "test1";
	public final static String ZOOKEEPER_HOST = "192.168.20.136";
	public final static String ZOOKEEPER_PORT = "2181";
	public final static String ZOOKEEPER_BROKER = "/brokers";
	public final static String KAFKA_BROKER_PORT = "9092"; 	
	 
	
	public final static String HDFS_DIR = "hdfs://192.168.20.136:8020/user/gpadmin/openbus";
	public final static String HDFS_USER = "gpadmin";
	
}
