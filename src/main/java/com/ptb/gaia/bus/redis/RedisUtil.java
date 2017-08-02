package com.ptb.gaia.bus.redis;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public final class RedisUtil {
    static Logger logger = Logger.getLogger(RedisUtil.class);


    //Redis服务器IP
    private static String ADDR;

    //Redis的端口号
    private static int PORT = 6379;

    //访问密码
    private static String AUTH = null;

    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_TOTAL = 8;

    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 3;

    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = 10000;

    private static int TIMEOUT = 10000;

    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;

    /**
     * 初始化Redis连接池
     */
    static {
        CompositeConfiguration conf = new CompositeConfiguration();
        try {
            PropertiesConfiguration defaultConf = new PropertiesConfiguration("bus-default.properties");
            conf.addConfiguration(defaultConf);
            PropertiesConfiguration UserConf1 = new PropertiesConfiguration("bus.properties");
            conf.addConfiguration(UserConf1);
        } catch (ConfigurationException e) {
            logger.warn("read bus.properties error ,check the config file");
        }

        ADDR = conf.getString("bus.redis.host", "127.0.0.1");
        PORT = conf.getInt("bus.redis.port", 6379);
        MAX_IDLE = conf.getInt("bus.redis.max.idle", 2);
        MAX_TOTAL = conf.getInt("bus.redis.max.total", 4);
        AUTH = conf.getString("bus.redis.password", null);
        TIMEOUT = conf.getInt("bus.redis.get.timeout", 10000);
        MAX_WAIT = conf.getInt("bus.redis.max.wait", 10000);

        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(MAX_TOTAL);
        config.setMaxWaitMillis(MAX_WAIT);
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH);
    }

    /**
     * 获取Jedis实例
     *
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 释放jedis资源
     *
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }

}