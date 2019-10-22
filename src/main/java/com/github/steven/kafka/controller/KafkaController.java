package com.github.steven.kafka.controller;

import com.ailk.common.data.IData;
import com.ailk.common.data.impl.DataMap;
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

            IData data = new DataMap();
            data.put("RULE_ID", "101010" + i);
            data.put("EXEC_TIME", "2019-10-21 16:00:01");
            data.put("COST_TIME", "2");
            data.put("SERIAL_NUMBER", "13812344321");
            data.put("EXEC_STAT", "0");
            data.put("ERROR_INFO", "该优惠[96036545]快装快修，超时送流量0元1GB流量日套餐在同一时期内不能重复订购");

            RuleExecInfoSender.send(data);
        }
        log.info("发送完毕。耗时: " + (System.currentTimeMillis() - start) + "ms");
        return true;
    }
}