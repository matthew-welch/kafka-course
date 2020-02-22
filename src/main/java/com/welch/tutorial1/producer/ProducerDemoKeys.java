package com.welch.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    /**
     * https://kafka.apache.org/documentation/#producerconfigs
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create Producer record
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;
            logger.info("key " + key);
            // id 0 - partition 1
            // id 1 - partition 0
            // id 2 - partition 2
            // id 3 - partition 0
            // id 4 - partition 2
            // id 5 - partition 2
            // id 6 - partition 0
            // id 7 - partition 2
            // id 8 - partition 1
            // id 9 - partition 2

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
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
            }).get(); // block send to make synchronous (not prod code!)
        }
        // force (or producer.close() will flush and close)
        producer.flush();
    }
}
