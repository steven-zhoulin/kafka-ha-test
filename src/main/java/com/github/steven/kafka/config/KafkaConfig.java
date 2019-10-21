package com.github.steven.kafka.config;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Steven
 * @date 2019-10-21
 */
@Configuration
public class KafkaConfig {

    @Bean
    public Producer<byte[], byte[]> kafkaProducer() {
        String brokerList = "192.168.37.131:9092";
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        Producer<byte[], byte[]> instance = new Producer(new ProducerConfig(props));
        return instance;
    }

    @Bean
    public LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue() {
        LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue = new LinkedBlockingQueue<>(10000);
        return linkedBlockingQueue;
    }

}
