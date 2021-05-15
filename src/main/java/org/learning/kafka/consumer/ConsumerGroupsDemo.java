package org.learning.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerGroupsDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroupsDemo.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-seven-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            logger.info("Total Records: {}", records.count());
            records.forEach(record -> {
                logger.info("New Record Received");
                logger.info("record Topic: {}", record.topic());
                logger.info("record Key: {}", record.key());
                logger.info("record Value: {}", record.value());
                logger.info("record Partition: {}", record.partition());
                logger.info("record Offset: {}", record.offset());
                logger.info("record Timestamp: {}", record.timestamp());
            });

        }
    }
}
