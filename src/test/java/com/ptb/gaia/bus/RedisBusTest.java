package com.ptb.gaia.bus;

import com.ptb.gaia.bus.message.Message;
import com.ptb.gaia.bus.message.MessageListener;
import com.ptb.gaia.bus.redis.RedisBus;
import com.ptb.gaia.bus.redis.RedisUtil;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eric on 16/1/9.
 */
public class RedisBusTest {
    static Logger logger = Logger.getLogger(RedisBusTest.class);
    Bus redisBus = new RedisBus();
    AtomicInteger recvNum = new AtomicInteger();


    @Before
    public void init() {
        logger.info("初始化REDIS-BUS总线");
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(Bus.UranusAsistant);
        jedis.del("ddd");
        redisBus.addConsumerTopic(Bus.UranusAsistant);
        redisBus.addConsumerTopic("ddd");
        redisBus.start(false, 4);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("REDIS-BUS初始化完成");
    }

    @Test()
    public void receiverListenerTest() {
        logger.info("测试:recerverListern,共发送20条消息到总线,监听器应收到20条消息");
        ReceiverListener receiverListener = new ReceiverListener() {
            public void receive(Bus bus, String recvBus, byte[] message) {
                if (recvBus.equals(Bus.UranusAsistant)) {
                    int i = recvNum.incrementAndGet();
                    //System.out.println(String.format("recv the %d message", i));
                    TestCase.assertEquals(recvBus, Bus.UranusAsistant);
                }
            }
        };

        redisBus.addRecvListener(receiverListener);

        for (int i = 0; i < 20; i++) {
            redisBus.send(Bus.UranusAsistant, "helloword".getBytes());
        }

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        TestCase.assertEquals(20, recvNum.get());
    }

    @Test
    public void messageListernTest() {
        logger.info("测试:messageListern,共发送10条消息到总线,监听器应收到10条消息");


        redisBus.addRecvMessageListener(new MessageListener() {
            public void receive(Bus bus, Message msg) {
                TestCase.assertEquals(1L == msg.getType(), true);
            }

            public boolean match(Message message) {
                return message.getType() == 1L;
            }
        });

        for (int i = 0; i < 10; i++) {
            redisBus.send(new Message("ser", "ddd", 1, "dddd", 222L, "2222"));
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @After
    public void deinit() {
        redisBus.over();
    }
}
