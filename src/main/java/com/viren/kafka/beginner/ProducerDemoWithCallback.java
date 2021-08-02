package com.viren.kafka.beginner;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		//Step1: To create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Step2: Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		
		for (int i=0; i<10; i++){
			//Step3: To create producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World" + Integer.toString(i));
			
			//Step4: To send the data(record)
			producer.send(record, new Callback() {	
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// execute every time when a record is successfully sent or exception occurred
					if (exception== null){
						//successfully sent message
						logger.info("Received New Metadata \n" + 
						"Topic:" + metadata.topic() + "\n" +
						"Partition" + metadata.partition() + "\n" +
						"Offset:" + metadata.offset() + "\n" +
						"Timestamp :" + metadata.timestamp() + "\n"
						);
					} else {
						//error in sending message
						logger.error("Error while producing", exception);
					}
				}
			});
			//To flush the data
			producer.flush();
		}
				
		//To flush and close
		producer.close();
	}
}
