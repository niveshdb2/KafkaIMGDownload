package com.github.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
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
		
		Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);
		
		String bootstrapServer =  "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String topic = "first_topic";
		
		Properties properties= new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId );
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
		
		//subscribe consumer to our topic 
		consumer.subscribe(Arrays.asList(topic));
		System.out.println("Consumer Started");
		
		while(true) {
			System.out.println("Consumer");
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
			if(records.count() == 0) {
				System.out.println("No records retrieved");
			}
			
			for(ConsumerRecord<String,String> record : records) {
				
				try {
					logger.info("keyyy:" +record.key() +" Valueeee:" +record.value());
					logger.info("partition: " +record.partition() + "OFFSet:" +record.offset());
					
					String value = record.value();
					
					logger.info("BasicBase64format = "+ value);
					
					//Getting MIME Decoder
					Base64.Decoder decoder = Base64.getMimeDecoder();
					
					//Decoding MIME encoded message
					byte[] barray = decoder.decode(value.getBytes());
					
					logger.info("Decoded message:" + barray);
					
					Files.write(Paths.get("/Users/nivesh/Documents/kafkaProject/Nivesh.png"), barray);
					System.out.println("IMAGE DOWNLOADED ..");
					
				}catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			try {
				Thread.sleep(10000);
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
