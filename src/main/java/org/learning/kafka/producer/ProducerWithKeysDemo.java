package org.learning.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeysDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeysDemo.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 20; i++) {
            String topic = "first_topic";
            String value = "hello world " + i + " from callback";
            String key = "id_" + i;
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: {}", key);
            producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                logger.info("New RecordMetaData Received: ");
                logger.info("Topic: {}", recordMetadata.topic());
                logger.info("Partition: {}", recordMetadata.partition());
                logger.info("Offset: {}", recordMetadata.offset());
                logger.info("Timestamp: {}", recordMetadata.timestamp());
            }).get();
        }
//        producer.flush();
        producer.close();
    }

}