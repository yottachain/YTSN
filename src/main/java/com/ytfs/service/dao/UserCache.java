package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.yottachain.p2phost.utils.Base58;
import io.yottachain.ytcrypto.core.exception.YTCryptoException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class UserCache {

    private static final long MAX_SIZE = 5000000;
    private static final long READ_EXPIRED_TIME = 60;

    private static final Cache<String, User> users = CacheBuilder.newBuilder()
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static User getUser(String key) {
        return users.getIfPresent(key);
    }

    public static void putUser(String key, User user) {
        users.put(key, user);
    }

    public static User getUser(String key, int userId, String signData) throws ServiceException {
        User user = UserAccessor.getUser(userId);
        if (user == null) {
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        String pubkey = Base58.encode(user.getKUEp());
        String data = user.getUserID() + user.getUsername();
        try {
            boolean pass = io.yottachain.ytcrypto.YTCrypto.verify(pubkey, data.getBytes(Charset.forName("UTF-8")), signData);
            if (pass) {
                users.put(key, user);
                return user;
            } else {
                throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
            }
        } catch (YTCryptoException ex) {
            throw new ServiceException(ServiceErrorCode.SERVER_ERROR);
        }
    }

}
