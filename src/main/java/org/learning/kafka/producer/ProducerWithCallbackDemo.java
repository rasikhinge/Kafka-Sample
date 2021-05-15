package org.learning.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "hello world " + i + " from callback");

            Callback callback = new CallBackImpl();
            producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                logger.info("New RecordMetaData Received: ");
                logger.info("Topic: {}", recordMetadata.topic());
                logger.info("Partition: {}", recordMetadata.partition());
                logger.info("Offset: {}", recordMetadata.offset());
                logger.info("Timestamp: {}", recordMetadata.timestamp());
            });
        }
//        producer.flush();
        producer.close();
    }

}

class CallBackImpl implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(CallBackImpl.class);

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("Into the callback on complete method");
        logger.info("New RecordMetaData Received: ");
        logger.info("Topic: {}", recordMetadata.topic());
        logger.info("Partition: {}", recordMetadata.partition());
        logger.info("Offset: {}", recordMetadata.offset());
        logger.info("Timestamp: {}", recordMetadata.timestamp());
    }
}
