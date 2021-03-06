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

    private static final Cache<String, UserEx> users = CacheBuilder.newBuilder()
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static User getUser(String key) {
        UserEx ue = users.getIfPresent(key);
        if (ue != null) {
            return ue.user;
        } else {
            return null;
        }
    }

    public static UserEx getUserEx(String key) {
        return users.getIfPresent(key);
    }

    public static void putUser(String key, User user, int keyNumber) {
        UserEx ue = new UserEx();
        ue.user = user;
        ue.keyNumber = keyNumber;
        users.put(key, ue);
    }

    public static User getUser(String key, int userId, String signData, int keyNumber) throws ServiceException {
        User user = UserAccessor.getUser(userId);
        if (user == null) {
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        String pubkey = Base58.encode(user.getKUEp()[keyNumber]);
        String data = user.getUserID() + user.getUsername() + keyNumber;
        try {
            boolean pass = io.yottachain.ytcrypto.YTCrypto.verify(pubkey, data.getBytes(Charset.forName("UTF-8")), signData);
            if (pass) {
                UserEx ue = new UserEx();
                ue.user = user;
                ue.keyNumber = keyNumber;
                users.put(key, ue);
                return user;
            } else {
                throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
            }
        } catch (YTCryptoException ex) {
            throw new ServiceException(ServiceErrorCode.SERVER_ERROR);
        }
    }

    public static class UserEx {

        public User user;
        public int keyNumber;
    }
}
