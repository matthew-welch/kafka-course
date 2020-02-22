package com.welch.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    /**
     * https://kafka.apache.org/documentation/#producerconfigs
     */
    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create Producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world " + i);
            // send data - asynchronous
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    // success
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    // fail
                    logger.error("Error: ", e);
                }
            });
        }
        // force (or producer.close() will flush and close)
        producer.flush();
    }
}
