package com.viren.kafka.beginner;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		
		String bootstrapServers = "127.0.0.1:9092";
		
		//Step1: To create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Step2: Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Step3: To create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");
		
		//Step4: To send the data(record)
		producer.send(record);
		
		//To flush the data
		producer.flush();
		
		//To flush and close
		producer.close();
		
	}
}
