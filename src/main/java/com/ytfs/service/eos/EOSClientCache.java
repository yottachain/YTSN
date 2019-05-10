package com.ytfs.service.eos;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.jafka.jeos.EosApi;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class EOSClientCache {

    private static final long MAX_SIZE = 100000;
    public static final long EXPIRED_TIME = 120;

    private static final Cache<ObjectId, EosApi> clients = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.SECONDS)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.SECONDS)
            .maximumSize(MAX_SIZE)
            .build();

  
    public static void putClient(ObjectId key, EosApi value) {
        clients.put(key, value);
    }

    public static EosApi getClient(ObjectId key) {
        EosApi api = clients.getIfPresent(key);
        if (api != null) {
            clients.invalidate(key);
        }
        return api;
    }

}
