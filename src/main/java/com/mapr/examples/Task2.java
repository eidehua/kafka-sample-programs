package com.mapr.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class Task2 {
	// Read each message from the Kafka topic called "mock_twitter_stream".
	// If the message is one of the 9 non-Tweet messages described above then ignore the message.
	// Otherwise write the message to a Kafka topic called "spark_input"

	// needs to consume message, then also produce after

    public static void main(String[] args) throws IOException {
        // set up house-keeping
        ObjectMapper mapper = new ObjectMapper();

        // Setup the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList("mock_twitter_stream", "test"));
        
        // Setup the producer
	    KafkaProducer<String, String> producer;
	    try (InputStream props = Resources.getResource("producer.props").openStream()) {
	        Properties properties = new Properties();
	        properties.load(props);
	        producer = new KafkaProducer<>(properties);
	    }
	    
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
                switch (record.topic()) {
            		// Read each message from the Kafka topic called "mock_twitter_stream".
                    case "mock_twitter_stream":
                        // the send time is encoded inside the message
                        JsonNode msg = mapper.readTree(record.value());
                        if (msg.get("limit") == null) {
                        	//Otherwise write the message to a Kafka topic called "spark_input"
                        	// I basically check the JSON to see if it has the limit key -- if it does not, then it is a tweet
                            producer.send(new ProducerRecord<String, String>(
                                    "spark_input", record.value()));
 
                        } else {
                        	//If the message is one of the 9 non-Tweet messages described above then ignore the message.
                       }
                        break;
                    case "test":
                        break;
                    default:
                        throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                }
            }
        }
    }
}
