package com.ytfs.service.dao;

import static com.ytfs.service.dao.RedisSource.REDIS_THREAD_LOCAL;
import static com.ytfs.service.dao.RedisSource.counter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class RedisExpiredChecker extends Thread {
    
    private static final Logger LOG = Logger.getLogger(RedisExpiredChecker.class);
    
    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                Thread.sleep(1000 * 60 * 10);
                check();
            } catch (InterruptedException ex) {
                break;
            } catch (Exception ex) {
                LOG.error("", ex);
            }
        }
        
    }
    
    public void close() {
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException ex) {
        }
        synchronized (REDIS_THREAD_LOCAL) {
            for (RedisSource rs : REDIS_THREAD_LOCAL) {
                rs.close();
            }
        }
    }
    
    private void check() {
        synchronized (REDIS_THREAD_LOCAL) {
            List<RedisSource> ls = new ArrayList(REDIS_THREAD_LOCAL);
            for (RedisSource rs : ls) {
                if (rs.isExpired()) {
                    REDIS_THREAD_LOCAL.remove(rs);
                    counter--;
                    rs.close();
                }
            }
        }
    }
}
