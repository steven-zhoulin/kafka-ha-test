package com.github.steven.kafka.util;

import com.ailk.common.data.IData;
import com.ailk.mq.util.KafkaUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 规则执行信息发送器
 *
 * @author Steven
 * @date 2019-10-21
 */
public class RuleExecInfoSender {

    /**
     * Kafka Topic
     */
    private static String topicName = "topic-crm-rule-execinfo";

    /**
     * 发送单条数据超时时间，单位：毫秒
     */
    private static int timeout = 1;

    /**
     * 本地有界队列，作用是当 Kafka 宕机时，不影响业务继续运行。
     */
    private static final LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue = new LinkedBlockingQueue<>(10000);

    /**
     * 消息生产者
     */
    private static Producer<byte[], byte[]> producerInstance = KafkaUtil.getProducerInstance();

    /**
     * 消息 Id
     */
    private static final AtomicLong messageId = new AtomicLong(1);

    /**
     * 采用后台独立线程送 Kafka
     */
    static {
        Sender sender = new Sender();
        sender.setDaemon(true);
        sender.start();
    }

    /**
     * 提供给业务方的接口
     * 样本数据：
     * {"RULE_ID": "101010", "EXEC_TIME": "2019-10-21 16:00:01", "COST_TIME": "2", "SERIAL_NUMBER": "13812341234", "EXEC_STAT": "0"}
     * {"RULE_ID": "202020", "EXEC_TIME": "2019-10-21 16:00:01", "COST_TIME": "2", "SERIAL_NUMBER": "13812341234", "EXEC_STAT": "1", "ERROR_INFO": "该优惠[96036545]快装快修，超时送流量0元1GB流量日套餐在同一时期内不能重复订购"}
     *
     * 规则编码: RULE_ID
     * 执行时间: EXEC_TIME (规则执行的时间点)
     * 执行耗时: COST_TIME
     * 用户号码: SERIAL_NUMBER
     * 执行状态: EXEC_STAT (0代表成功，!0失败)
     * 错误信息: ERROR_INFO (只有当执行错误的情况下才有数据)
     *
     * @param data IData 数据集
     */
    public static final void send(IData data) {
        String messageValue = data.toString();
        send(messageValue);
    }

    /**
     * 提供给业务方的接口
     *
     * @param messageValue 消息内容，格式: Json
     */
    public static final void send(String messageValue) {
        long key = messageId.getAndIncrement();

        KeyedMessage keyedMessage = new KeyedMessage(topicName, String.valueOf(key).getBytes(), messageValue.getBytes());
        try {
            linkedBlockingQueue.offer(keyedMessage, timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 独立线程送 Kafka
     */
    private static class Sender extends Thread {
        private Sender() {
        }

        @Override
        public void run() {
            while (true) {
                try {
                    producerInstance.send(linkedBlockingQueue.take());
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                } catch (RuntimeException re) {
                    re.printStackTrace();
                }
            }
        }
    }
}
