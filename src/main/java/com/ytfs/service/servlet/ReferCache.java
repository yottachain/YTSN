package com.ytfs.service.servlet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.packet.ObjectRefer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class ReferCache {

    private static final long MAX_SIZE = 500000;
    private static final long EXPIRED_TIME = 3;

    private static final Cache<ObjectId, Refer> refers = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static List<ObjectRefer> getRefersCache(ObjectId VNU, int userid) throws ServiceException {
        Refer cache = refers.getIfPresent(VNU);
        if (cache == null) {
            cache = loadRefersCache(VNU, userid);
        }
        return cache.objRefers;
    }

    private static Refer loadRefersCache(ObjectId VNU, int userid) throws ServiceException {
        ObjectMeta meta = queryObjectMeta(VNU, userid);
        Refer cache = new Refer();
        cache.length = meta.getLength();
        cache.objRefers = ObjectRefer.parse(meta.getBlocks(),meta.getBlockList());
        refers.put(VNU, cache);
        return cache;
    }

    public static void delRefersCache(ObjectId VNU) {
        refers.invalidate(VNU);
    }

    public static long getObjectLength(ObjectId VNU, int userid) throws ServiceException {
        Refer cache = refers.getIfPresent(VNU);
        if (cache == null) {
            cache = loadRefersCache(VNU, userid);
        }
        return cache.length;
    }

    private static ObjectMeta queryObjectMeta(ObjectId VNU, int userID) throws ServiceException {
        ObjectMeta meta = ObjectAccessor.getObject(VNU);
        if (meta == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        } else {
            if (meta.getUserID() != userID) {
                throw new ServiceException(INVALID_UPLOAD_ID);
            } else {
                return meta;
            }
        }
    }

    private static class Refer {

        public List<ObjectRefer> objRefers;
        public long length;

    }
}
