package com.github.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		
		String bootstrapServers = "127.0.0.1:9092";
		
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create a producer record
		//ProducerRecord<String, String> record =
		//		new ProducerRecord<String, String>("first_topic","Hey testing 23 ");
		try {
			System.out.println("Start");
			byte[] barray = Files.readAllBytes(Paths.get("/Users/nivesh/Documents/MyImage/MyPic.png"));
			System.out.println("barray=>"+barray);
			String BasicBase64format = Base64.getMimeEncoder().encodeToString(barray);
			System.out.println("BasicBase64format =>"+ BasicBase64format);
			
			String key = UUID.randomUUID().toString();
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",key,BasicBase64format);
			producer.send(record);
			System.out.println("Completed...");
			
			producer.flush();
			producer.close();
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
		

	}

}
