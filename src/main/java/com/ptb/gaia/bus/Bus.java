package com.ptb.gaia.bus;

import com.ptb.gaia.bus.message.Message;
import com.ptb.gaia.bus.message.MessageConverter;
import com.ptb.gaia.bus.message.MessageListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by eric on 16/1/8.
 */
public abstract class Bus implements Runnable {
    static Logger logger = Logger.getLogger(Bus.class);
    static Logger recvLogger = Logger.getLogger("recv.message");
    static Logger sendLogger = Logger.getLogger("send.message");

    public static String UranusServer = "uranusServer";
    public static String UranusAsistant = "uranusAsistant";
    public static String UranusClawer = "uranusClawer";
    public static String GaiaServer = "gaiaServer";


    public AtomicLong recvMsgCount = new AtomicLong(0L);


    protected ExecutorService executors = Executors.newCachedThreadPool();

    protected List<ReceiverListener> recvListeners = Collections.synchronizedList(new ArrayList<ReceiverListener>());
    protected List<MessageListener> recvMessageListeners = Collections.synchronizedList(new ArrayList<MessageListener>());

    protected Set<String> recvTopics = new HashSet<String>();
    private boolean isRun;
    private int recvThreadNum;
    private boolean enableOutputLog = true;

    public boolean isRun() {
        return isRun;
    }

    public void send(String dest, byte[] e) {
        if (enableOutputLog == true) {
            sendLogger.debug(new String(e));
        }
    }

    abstract public void send(Message message);


    abstract public byte[] recv(String recvTopic);

    public void addConsumerTopic(String topicName) {
        if (isRun == true) {
            throw new BusException(String.format("running state can't add consume topic name[%s]", topicName));
        }
        recvTopics.add(topicName);
    }

    public void addRecvListener(ReceiverListener receiverListener) {
        this.recvListeners.add(receiverListener);
    }

    public void addRecvMessageListener(MessageListener receiverListener) {
        this.recvMessageListeners.add(receiverListener);
    }

    public Set<String> getRecvTopics() {
        return recvTopics;
    }

    abstract public long getNoConsumeSize(String topic) throws Exception;

    private void recvHandle(String recvBus, byte[] msg) {
        for (int i = 0; i < recvListeners.size(); i++) {
            try {
                recvListeners.get(i).receive(this, recvBus, msg);
            } catch (Exception e) {
                logger.error(String.format("recvListeners error [%s]", recvListeners.get(i).getClass(), e), e);
            }
        }
        if (recvMessageListeners.size() > 0) {
            Message message = MessageConverter.convertToMessage(msg);
            for (int i = 0; i < recvMessageListeners.size(); i++) {
                try {
                    MessageListener recvMessageListener = recvMessageListeners.get(i);
                    if (recvMessageListener != null && recvMessageListener.match(message)) {
                        recvMessageListener.receive(this, message);
                    }
                } catch (Exception e) {
                    logger.error(String.format("Error Msg Formate............From Bus[%s]", recvBus), e);
                }
            }
        }

    }

    public void start(boolean isDeamon, int recvThreadNum) {
        if (recvTopics.size() <= 0 || recvThreadNum <= 0) {
            logger.warn("No Set recv topic or recv threadnum .......... ");
        }
        this.isRun = true;
        this.recvThreadNum = recvThreadNum;
        for (int i = 0; i < recvThreadNum; i++) {
            executors.submit(this);
            logger.info(String.format("starting the [%d] recv thread", i + 1));
        }

        logger.info(String.format("bus receiver start......[OK]"));

        if (isDeamon == true) {
            while (isRun) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    final public void run() {
        while (isRun) {
            try {
                for (String topic : recvTopics) {
                    byte[] msg = recv(topic);
                    if (msg != null) {
                        if (enableOutputLog == true) {
                            long l = recvMsgCount.incrementAndGet();
                            recvLogger.debug(new String(msg));
                        }

                        recvHandle(topic, msg);
                    } else {
                        Thread.sleep(100);
                    }
                }

            } catch (Exception e) {
                logger.error("", e);
            }
        }
        logger.info("bus recv thread exit.............................");
    }


    public void over() {
        this.isRun = false;
    }

    public void showdown() {
        isRun = false;
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
