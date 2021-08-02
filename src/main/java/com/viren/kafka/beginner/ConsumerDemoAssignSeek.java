package com.viren.kafka.beginner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		String topic = "first_topic";
		
		//Step1: Setup the properties for consumer
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//earliest: read from beginning of the topic
		//latest: read from the new message of the topic
		
		//Step2: Create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//Step3: Assign and seek to replay the data
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partitionToReadFrom));
		long offsetToReadFrom = 15L;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessageReadSoFar = 0;
		
		while(keepOnReading){
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records){
				numberOfMessageReadSoFar += 1;
				logger.info("Key: " + record.key() + ", Value:" + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				if(numberOfMessageReadSoFar >= numberOfMessagesToRead){
					keepOnReading = false;
					break;
				}
			}
		}
		
		logger.info("Exiting the applicationi");

	}

}
