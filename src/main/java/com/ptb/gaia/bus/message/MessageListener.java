package com.ptb.gaia.bus.message;

/**
 * Created by eric on 16/1/11.
 */

import com.ptb.gaia.bus.Bus;

import java.util.EventListener;

public interface MessageListener extends EventListener {
    void receive(Bus bus, Message msg);
    boolean match(Message message);
}
