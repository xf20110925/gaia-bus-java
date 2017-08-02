package com.ptb.gaia.bus.redis;

import com.ptb.gaia.bus.Bus;
import com.ptb.gaia.bus.message.Message;
import com.ptb.gaia.bus.message.MessageConverter;
import redis.clients.jedis.Jedis;

/**
 * Created by eric on 16/1/9.
 */
public class RedisBus extends Bus {

    @Override
    public void send(String target, byte[] message) {
        Jedis jedis = RedisUtil.getJedis();
        try {
            long a = jedis.lpush(target.getBytes(), message);
        } finally {
            if (jedis != null) {
                RedisUtil.returnResource(jedis);
            }
        }
    }

    @Override
    public void send(Message message) {
        send(message.getDest(), MessageConverter.convertToBytes(message));
    }

    @Override
    public byte[] recv(String recvBus) {
        Jedis jedis = RedisUtil.getJedis();
        try {
            return jedis.rpop(recvBus.getBytes());
        } finally {
            if (jedis != null) {
                RedisUtil.returnResource(jedis);
            }
        }
    }

    @Override
    public long getNoConsumeSize(String topic) {
        Jedis jedis = RedisUtil.getJedis();
        try {
            return jedis.llen(topic);
        } finally {
            if (jedis != null) {
                RedisUtil.returnResource(jedis);
            }
        }
    }
}
