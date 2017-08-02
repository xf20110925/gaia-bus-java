package com.ptb.gaia.bus.message;


import com.alibaba.fastjson.JSON;

import java.lang.reflect.Type;

/**
 * Created by eric on 16/1/11.
 */
public class MessageConverter {
    public static Message convertToMessage(byte[] msg) {
        Message message = JSON.parseObject(msg, Message.class);
        message.setRaw(msg);
        return message;
    }

    public static <T extends Message> T convertToMessage(byte[] msg, Type clazz) {
        return JSON.parseObject(msg, clazz);
    }

    public static byte[] convertToBytes(Message message) {
        return JSON.toJSONString(message).getBytes();
    }
}
