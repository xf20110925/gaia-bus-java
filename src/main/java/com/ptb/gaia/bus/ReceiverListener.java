package com.ptb.gaia.bus;

import java.util.EventListener;

/**
 * Created by eric on 16/1/8.
 */
public interface ReceiverListener extends EventListener {
    void receive(Bus bus, String recvBus, byte[] message);
}
