package com.ytfs.service.dao;

import com.mongodb.MongoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

public class RedisSource {

    static final List<RedisSource> REDIS_THREAD_LOCAL = new ArrayList();
    static int counter = 0;

    private static RedisSource newInstance() {
        synchronized (REDIS_THREAD_LOCAL) {
            if (REDIS_THREAD_LOCAL.isEmpty()) {
                try {
                    RedisSource source = new RedisSource();
                    counter++;
                    return source;
                } catch (Exception r) {
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException ex) {
                    }
                    throw new JedisException(r.getMessage());
                }
            } else {
                return REDIS_THREAD_LOCAL.remove(0);
            }
        }
    }

    public static void backSource(RedisSource source) {
        synchronized (REDIS_THREAD_LOCAL) {
            REDIS_THREAD_LOCAL.add(source);
        }
    }

    public static RedisSource getSource() {
        return newInstance();
    }

    private static final Logger LOG = Logger.getLogger(RedisSource.class);
    private BasicCommands jedis = null;
    private long activeTime;

    private RedisSource() throws JedisException {
        String path = System.getProperty("mongo.conf", "conf/mongo.properties");
        try (InputStream inStream = new FileInputStream(path)) {
            Properties p = new Properties();
            p.load(inStream);
            init(p);
            activeTime = System.currentTimeMillis();
        } catch (Exception e) {
            close();
            throw e instanceof JedisException ? (JedisException) e : new JedisException(e.getMessage());
        }
    }

    public void close() {
        if (getJedis() != null) {
            if (getJedis() instanceof JedisCluster) {
                try {
                    ((JedisCluster) getJedis()).close();
                } catch (IOException ex) {
                }
            } else {
                ((Jedis) getJedis()).close();
            }
        }
    }

    private void init(Properties p) {
        String hostlist = p.getProperty("redislist");
        if (hostlist == null || hostlist.trim().isEmpty()) {
            throw new MongoException("MongoSource.properties文件中没有指定redislist");
        }
        String[] hosts = hostlist.trim().split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        for (String host : hosts) {
            try {
                if (host != null) {
                    String[] addr = host.trim().split("\\:");
                    HostAndPort hostAndPort = new HostAndPort(addr[0], Integer.parseInt(addr[1]));
                    nodes.add(hostAndPort);
                    LOG.info("[" + hostAndPort.toString() + "]添加到服务器列表中...");
                }
            } catch (Exception r) {
            }
        }
        if (nodes.isEmpty()) {
            throw new MongoException("MongoSource.properties文件中没有指定redislist");
        }
        if (nodes.size() < 3) {
            HostAndPort hp = nodes.iterator().next();
            jedis = new Jedis(hp.getHost(), hp.getPort());
        } else {
            jedis = new JedisCluster(nodes);
        }
        LOG.info("连接服务器成功!");
    }

    /**
     * @return the jedis
     */
    public BasicCommands getJedis() {
        activeTime = System.currentTimeMillis();
        return jedis;
    }

    /**
     * @return the activeTime
     */
    public boolean isExpired() {
        return System.currentTimeMillis() - activeTime > 1000 * 60 * 3;
    }

}
