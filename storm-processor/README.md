storm-processor
====================

## Project Openbus Storm Processor (RAW)

Kafka are consumed for processing messages with Trident Storm 

Is necesary install the proyect storm-kafka-0.8-plus (https://github.com/wurstmeister/storm-kafka-0.8-plus) in the repository of maven
- publish storm-kafka-0.8-plus to local maven repo
	- ```mvn install:install-file -Dfile=libs/storm-kafka-0.8-plus-0.1.0-SNAPSHOT.jar -DgroupId=storm -DartifactId=storm-kafka-0.8-plus -Dversion=0.1.0-SNAPSHOT -Dpackaging=jar```

# Compile 
	- ```mvn clean compile``` 

# Execute example TridentWordCount in local 
	- ```mvn exec:java -Dexec.mainClass="com.produban.openbus.processor.example.TridentWordCount"```


# Verification cluster Storm with Trident
	- ```mvn pakcage```
	- ```storm jar target/storm-processor-0.0.1-jar-with-dependencies.jar com.produban.openbus.processor.example.TridentSocialNetworkTest socialnetwork```