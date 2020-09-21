# Kafka Twitter Producer  
##### Kafka Producer to stream tweets using Twitter API (in Scala)  
  
This SBT project is used to stream tweets to Kafka based on the topic(s) passed as argument(s).  
  
The arguments required to run the JAR file are as follows:  
1) Location of Kafka `.properties` file:  
The `.properties` file should contain the following properties:  
  
| Property                      | Description                                            |  
| :---------------------------- | :----------------------------------------------------- |  
| `twitter.api.key`             | Twitter API Key                                        |  
| `twitter.api.secret`          | Twitter API Key Secret                                 |  
| `twitter.token`               | Twitter API Token                                      |  
| `twitter.token.secret`        | Twitter API Token Secret                               |  
| `bootstrap.servers`           | Kafka Bootstrap Server(s) (comma-separated)            |  
| `group.id`                    | Topic Group ID                                         |  
| `enable.auto.commit`          | Enable Topic Auto Commit                               |  
| `auto.commit.interval.ms`     | Topic Auto Commit Interval in ms                       |  
| `key.deserializer`            | Fully Qualified Class Name of Kafka Key Deserializer   |  
| `value.deserializer`          | Fully Qualified Class Name of Kafka Value Deserializer |  
| `key.serializer`              | Fully Qualified Class Name of Kafka Key Serializer     |  
| `value.serializer`            | Fully Qualified Class Name of Kafka Value Serializer   |  
| `enable.idempotence`          | Enable Producer Idempotence, default = true            |  
| `compression.type`            | Producer Data Compression Algorithm, default = snappy  |  
| `linger.ms`                   | Producer Linger in ms, default = 20                    |  
| `batch.size`                  | Producer Message Batch Size, default = 65536           |  
  
Example:  
`twitter.api.key=TwitterApiKey`  
`twitter.api.secret=TwitterApiKeySecret`  
`twitter.token=TwitterApiToken`  
`twitter.token.secret=TwitterApiTokenSecret`  
`bootstrap.servers=localhost:9092,localhost:9093`  
`group.id=twitter-producer`  
`enable.auto.commit=true`  
`auto.commit.interval.ms=1000`  
`key.deserializer=org.apache.kafka.common.serialization.StringDeserializer`  
`value.deserializer=org.apache.kafka.common.serialization.StringDeserializer`  
`key.serializer=org.apache.kafka.common.serialization.StringSerializer`  
`value.serializer=org.apache.kafka.common.serialization.StringSerializer`  
`enable.idempotence=true`  
`compression.type=snappy`  
`linger.ms=20`  
`batch.size=65536`  
  
2) Pipe-separated list of topics:  
  The list of topics/keywords to subscribe to.  
  
Syntax:  
`java -jar <JAR_file_build> <properties_file> <list_of_topics>`  
  
Example:  
`java -jar /path/to/KafkaTwitterProducer.jar /path/to/kafka.properties "apache kafka|apache spark|scala"`  
  
`java -jar /path/to/KafkaTwitterProducer.jar /path/to/kafka.properties "apache kafka"`  
  
Note:  
This project was built with Scala version 2.12.11, Java 8 (u251) and sbt version 1.3.13.  
This project uses Kafka Clients API v2.5.0 of Apache Kafka for producing tweets, and Hosebird Client Core API v2.2.0 for connecting to Twitter.  
