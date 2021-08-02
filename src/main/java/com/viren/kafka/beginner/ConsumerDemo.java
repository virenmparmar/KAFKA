package com.viren.kafka.beginner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId ="my-sixth-application";
		
		//Step1: Setup the properties for consumer
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//earliest: read from beginning of the topic
		//latest: read from the new message of the topic
		
		//Step2: Create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//Step3: Subscribe the consumer to the topic
		consumer.subscribe(Arrays.asList("first_topic"));
		// Use list to subscribe to multiple topics. 
		//To subscribe to a single topic you can use Coolections.Singleton 
		
		//Step4: Keep polling to read
		while(true){
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records){
				logger.info("Key: " + record.key() + ", Value:" + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
		}

	}

}
