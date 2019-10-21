package com.github.steven.kafka.util;

import com.ailk.mq.util.KafkaUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Steven
 * @date 2019-10-21
 */
public class RuleExecInfoSender {

    private static String topicName = "topic-crm-rule-execinfo";
    private static int timeout = 1;
    private static final LinkedBlockingQueue<KeyedMessage> linkedBlockingQueue = new LinkedBlockingQueue<>(10000);

    private static Producer<byte[], byte[]> producerInstance = KafkaUtil.getProducerInstance();
    private static final AtomicLong messageId = new AtomicLong(1);

    static {
        Sender sender = new Sender();
        sender.setDaemon(true);
        sender.start();
    }

    /**
     * 提供给业务方的接口
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

    private static class Sender extends Thread {
        private Sender() {
        }

        @Override
        public void run() {
            while (true) {
                try {
                    producerInstance.send(linkedBlockingQueue.take());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (RuntimeException re) {
                    re.printStackTrace();
                }
            }
        }
    }
}
