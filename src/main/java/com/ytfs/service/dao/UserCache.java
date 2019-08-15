package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.jafka.jeos.util.Base58;
import java.util.concurrent.TimeUnit;

public class UserCache {

    private static final long MAX_SIZE = 100000;
    private static final long WRITE_EXPIRED_TIME = 10;
    private static final long READ_EXPIRED_TIME = 10;

    private static final Cache<String, User> users = CacheBuilder.newBuilder()
            .expireAfterWrite(WRITE_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static User getUser(String key) {
        User user = users.getIfPresent(key);
        if (user == null) {
            user = UserAccessor.getUser(Base58.decode(key));
            if (user != null) {
                users.put(key, user);
            }
        }
        return user;
    }

}
