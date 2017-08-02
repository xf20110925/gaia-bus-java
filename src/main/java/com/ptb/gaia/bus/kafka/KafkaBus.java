package com.ptb.gaia.bus.kafka;

import com.alibaba.fastjson.JSON;
import com.ptb.gaia.bus.Bus;
import com.ptb.gaia.bus.message.Message;
import com.ptb.gaia.bus.message.MessageConverter;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by eric on 16/1/15.
 */
public class KafkaBus extends Bus {
    static Logger logger = Logger.getLogger(KafkaBus.class);
    private Properties props;
    private Producer<byte[], byte[]> producer;
    private ConsumerConnector consumer;

    java.util.Map<String, BlockingQueue<byte[]>> topicQueueMap = new HashMap();
    private int consumerThread;

    public int getConsumerThread() {
        return consumerThread;
    }

    public void setConsumerThread(int consumerThread) {
        this.consumerThread = consumerThread;
    }

    private static Properties ConvertConfToProperties(Configuration conf) {
        Iterator keys = conf.getKeys();
        Properties props = new Properties();

        while (keys.hasNext()) {
            String key = String.valueOf(keys.next());
            props.put(key,conf.getProperty(key));
        }
        return props;
    }

    private void loadPropsFromPropertisFile() {
        props = new Properties();
        try {
            PropertiesConfiguration propsConf = new PropertiesConfiguration();
            propsConf.setListDelimiter('^');
            propsConf.load("bus-default.properties");

            props.putAll(ConvertConfToProperties(propsConf));
        } catch (Exception e) {
        }
        try {
            PropertiesConfiguration pConf = new PropertiesConfiguration();
            pConf.setListDelimiter('^');
            pConf.load("bus.properties");

            props.putAll(ConvertConfToProperties(pConf));
        } catch (Exception e) {
            logger.warn("custom bus.properties no exist");
        }


    }


    public KafkaBus() {
        loadPropsFromPropertisFile();
    }

    public KafkaBus(String topic) {
        recvTopics.add(topic);
        loadPropsFromPropertisFile();
    }

    public KafkaBus(String topic, Properties properties) {
        recvTopics.add(topic);
        loadPropsFromPropertisFile();
        props.putAll(properties);
    }

    public KafkaBus(Properties properties) {
        loadPropsFromPropertisFile();
        props.putAll(properties);
    }

    private void initProduct() {
        producer = new KafkaProducer(props);
    }


    private void initConsumer(int consumerThreadNum) {
        //初始化CONSUMER
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        for (String topic : recvTopics) {
            topicQueueMap.put(topic, new LinkedBlockingQueue(consumerThreadNum * 1));
            topicCountMap.put(topic, new Integer(consumerThreadNum));
        }
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        //启动CONSUMER WORK
        consumerMap.forEach(((topic, streams) -> {
            final int[] threadNum = {0};
            streams.forEach((stream) -> {
                executors.submit(new MessageConsumer(threadNum[0], topic, stream, (msg) -> {
                    try {
                        BlockingQueue<byte[]> topicQueue = topicQueueMap.get(topic);
                        if (topicQueue != null) {
                            topicQueue.put((byte[]) msg);
                        } else {
                            logger.error("没有对应的队列来接收TOPIC");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }));
                threadNum[0]++;
            });
        }));
    }


    @Override
    public void start(boolean isDeamon, int recvThreadNum) {
        initProduct();

        if (this.consumerThread == 0) {
            this.consumerThread = recvThreadNum;
        }

        if (this.consumerThread != 0) {
            initConsumer(consumerThread);
        }
        super.start(isDeamon, recvThreadNum);

    }

    @Override
    public void send(String dest, byte[] e) {
        super.send(dest, e);
        producer.send(new ProducerRecord<byte[], byte[]>(dest, e), (metadata, exception) -> {
            if (exception != null) {
                logger.error(JSON.toJSON(metadata), exception);
            }
        });
    }

    @Override
    public void send(Message message) {
        byte[] bytes = MessageConverter.convertToBytes(message);
        this.send(message.getDest(), bytes);
    }


    @Override
    public byte[] recv(String recvTopic) {
        //接收一个线程传来的消息内容
        byte[] msg = null;
        BlockingQueue<byte[]> topicQueue = topicQueueMap.get(recvTopic);

        try {
            if (topicQueue != null) {
                msg = topicQueue.poll(100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {

        }
        //System.out.println(new String(msg));
        return msg;

    }


    @Override
    public long getNoConsumeSize(String topic) throws Exception {
        String brokers = props.getProperty("bootstrap.servers");
        String zookeepers = props.getProperty("zookeeper.connect");
        String group = props.getProperty("group.id");
        return KafkaUtil.getRestMessge(brokers, zookeepers, 9092, topic, group);
    }


    @Override
    public void showdown() {

        try {
            if(consumer != null){
                consumer.shutdown();
            }
            Thread.sleep(3000);
            super.showdown();
            Thread.sleep(3000);
            executors.shutdown();
            if (!executors.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                return;
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted during shutdown, exiting uncleanly");
            return;
        }

        logger.info("kafka bus normal shutdown ..............");
    }

}
