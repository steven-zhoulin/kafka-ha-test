package com.github.steven.kafka.controller;

import kafka.producer.KeyedMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Steven
 */
@Slf4j
@RestController
public class KafkaController {

    @Autowired
    private LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue;

    private static final int BATCH_SIZE = 100000;

    @GetMapping("/message/send")
    public boolean send() throws InterruptedException {

        log.info("15秒后开始发送...");
        TimeUnit.SECONDS.sleep(15);

        long start = System.currentTimeMillis();
        for (int i = 1; i <= BATCH_SIZE; i++) {
            if (i % 1000 == 0) {
                log.info("已发送: {} 条。", i);
            }

            byte[] messageId = ("rule-key: " + i).getBytes();
            byte[] messageValue = ("rule-value: " + i).getBytes();
            try {
                linkedBlockingQueue.offer(new KeyedMessage("topic-crm-rule-execinfo", messageId, messageValue), 1, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("发送完毕。耗时: " + (System.currentTimeMillis() - start) + "ms");
        return true;
    }
}