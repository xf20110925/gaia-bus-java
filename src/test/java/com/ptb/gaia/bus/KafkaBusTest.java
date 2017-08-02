package com.ptb.gaia.bus;

import com.ptb.gaia.bus.kafka.KafkaBus;
import com.ptb.gaia.bus.message.Message;
import com.ptb.gaia.bus.message.MessageListener;
import junit.framework.TestCase;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eric on 16/1/18.
 */
public class KafkaBusTest {
    static Logger logger = Logger.getLogger(KafkaBusTest.class);
    Bus bus;

    @Before
    public void init() throws ConfigurationException {
        logger.info("正在初始化KAFKA-BUS总线");
        bus = new KafkaBus("mcinfo2");
        bus.start(false, 2);
        logger.info("初始化KAFKA-BUS总线完成");
    }

    @Test
    public void testSendAndReceiverListener() throws InterruptedException {
        logger.info("测试:recerverListern,共发送10条消息到总线,监听器应收到10条消息");
        AtomicInteger recvNum = new AtomicInteger();
        bus.addRecvListener((bus1, recvBus, message) -> {
            recvNum.incrementAndGet();
        });
        for (int i = 0; i < 10; i++) {
            bus.send(new Message("test", "mcinfo2", 1, "hahaha", "haha"));
        }
        Thread.sleep(2000);
        TestCase.assertEquals(recvNum.get(), 10);
    }

    @Test
    public void testSendAndMessageListener() throws InterruptedException {
        logger.info("测试:messageListern,共发送20条消息到总线(分别类型为type1,type2),监听器接收type2," +
                "应收到10条type2消息");
        AtomicInteger recvNum = new AtomicInteger();
        bus.addRecvMessageListener(new MessageListener() {
            @Override
            public void receive(Bus bus, Message msg) {
                recvNum.incrementAndGet();
            }

            @Override
            public boolean match(Message message) {
                return message.getType() == 1L;
            }
        });

        for (int i = 0; i < 10; i++) {
            bus.send(new Message("test", "test", 1, "hahaha", "haha"));
            bus.send(new Message("test", "test", 2, "hahaha", "haha"));
        }

        Thread.sleep(2000);
        TestCase.assertEquals(recvNum.get(), 10);
    }

    @After
    public void deinit() {
        bus.showdown();
    }

}
