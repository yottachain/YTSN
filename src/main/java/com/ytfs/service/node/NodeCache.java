package com.ytfs.service.node;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import java.util.concurrent.TimeUnit;

public class NodeCache {

    private static final long MAX_SIZE = 100000;
    private static final long SUP_EXPIRED_TIME = 10;
    private static final long EXPIRED_TIME = 5;

    private static final Cache<String, Integer> superNodes = CacheBuilder.newBuilder()
            .expireAfterWrite(SUP_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(SUP_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();
    private static final Cache<String, Integer> nodes = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static int getSuperNodeId(String key) throws NodeMgmtException {
        Integer id = superNodes.getIfPresent(key);
        if (id == null) {
            id = NodeManager.getSuperNodeIDByPubKey(key);
            superNodes.put(key, id);
        }
        return id;
    }

    public static int getNodeId(String key) throws NodeMgmtException {
        Integer id = nodes.getIfPresent(key);
        if (id == null) {
            id = NodeManager.getNodeIDByPubKey(key);
            superNodes.put(key, id);
        }
        return id;
    }
}
