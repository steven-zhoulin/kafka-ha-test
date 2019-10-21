package com.github.steven.kafka.controller;

import com.github.steven.kafka.util.RuleExecInfoSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * @author Steven
 */
@Slf4j
@RestController
public class KafkaController {

    private static final int BATCH_SIZE = 100000;

    @GetMapping("/message/send")
    public boolean send() throws InterruptedException {

        log.info("5 秒后开始发送...");
        TimeUnit.SECONDS.sleep(5);

        long start = System.currentTimeMillis();
        for (int i = 1; i <= BATCH_SIZE; i++) {
            if (i % 1000 == 0) {
                log.info("已发送: {} 条。", i);
            }

            RuleExecInfoSender.send("rule-value: " + i);
        }
        log.info("发送完毕。耗时: " + (System.currentTimeMillis() - start) + "ms");
        return true;
    }
}