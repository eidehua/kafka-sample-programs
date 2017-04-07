package com.mapr.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class Task1 {
	// Read each line of "tweets.txt".
	// Write each line to a Kafka topic called "mock_twitter_stream".
	// So this requires a KafkaProducer
    public static void main(String[] args) throws IOException {
	
	    KafkaProducer<String, String> producer;
	    try (InputStream props = Resources.getResource("task1.props").openStream()) {
	        Properties properties = new Properties();
	        properties.load(props);
	        producer = new KafkaProducer<>(properties);
	    }

        try {
        	try(BufferedReader br = new BufferedReader(new FileReader("src/main/resources/tweets.txt"))) {
        	    for(String line; (line = br.readLine()) != null; ) {
        	        // process the line.
                    producer.send(new ProducerRecord<String, String>(
                            "mock_twitter_stream", line));
        	    }
        	    // line is not visible here.
        	}
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }	    
    }
}
