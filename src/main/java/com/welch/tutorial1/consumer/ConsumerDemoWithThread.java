package com.welch.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        // laych for multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // consumer
        Runnable myConsumer = new ConsumerRunnable(latch, "first_topic");

        // start thread
        Thread myThread = new Thread(myConsumer);
        myThread.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumer).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application interrupted.", e);
            } finally {
                logger.error("Application closing.");
            }
            logger.info("Application has exited.");
        }));
    }

    private Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-sixth-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch, String topic) {
            this.latch = latch;

            Properties properties = initProperties();
            consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to our topics
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            // poll for data
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException we) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell main code we are done
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeup interrupts consumer.poll()
            // throws WakeUpException
            consumer.wakeup();
        }
    }
}
