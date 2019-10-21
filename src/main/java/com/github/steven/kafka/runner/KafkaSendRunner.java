package com.github.steven.kafka.runner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 后台执行线程
 *
 * @author Steven
 * @date 2019-10-21
 */
@Slf4j
//@Component
public class KafkaSendRunner implements CommandLineRunner {

    @Autowired
    private Producer<byte[], byte[]> kafkaProducer;

    @Autowired
    private LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue;

    @Override
    public void run(String... args) {
        log.info("KafkaSendRunner started. ");

        while (true) {
            try {
                kafkaProducer.send(linkedBlockingQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (RuntimeException re) {
                re.printStackTrace();
            }
        }
    }
}