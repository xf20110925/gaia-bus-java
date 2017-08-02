package com.ptb.gaia.bus.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.log4j.Logger;

import java.util.function.Consumer;


public class MessageConsumer implements Runnable {
    static Logger logger = Logger.getLogger(MessageConsumer.class);
    private final Consumer<Object> cb;
    private int threadNum;
    private String topic;
    private KafkaStream m_stream;

    public MessageConsumer(int threadNum, String topic, KafkaStream<byte[], byte[]> stream, Consumer<Object> cb) {
        this.topic = topic;
        this.m_stream = stream;
        this.cb = cb;
        this.threadNum = threadNum;
    }

    public void run() {
        logger.info(String.format("Topic [%s] thread [%s] init......", topic, threadNum));
        ConsumerIterator it = m_stream.iterator();
        while (it.hasNext()) {
            Object msg = it.next().message();
            cb.accept(msg);
        }
        logger.info(String.format("kafka consumer [topic:%s,threadNum:%d] exit.................", topic, threadNum));
    }
}