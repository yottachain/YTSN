package com.ytfs.service.servlet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.service.packet.bp.QueryObjectMetaReq;
import com.ytfs.service.packet.bp.QueryObjectMetaResp;
import com.ytfs.service.servlet.bp.QueryObjectMetaHandler;
import com.ytfs.common.ServiceException;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class CacheAccessor {

    private static final long OBJ_MAX_SIZE = 500000;
    private static final long OBJ_EXPIRED_TIME = 9;

    private static final Cache<ObjectId, UploadObjectCache> uploadObjects = CacheBuilder.newBuilder()
            .expireAfterAccess(OBJ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(OBJ_MAX_SIZE)
            .build();

    public static UploadObjectCache getUploadObjectCache(int userid, ObjectId VNU) throws ServiceException {
        UploadObjectCache cache = uploadObjects.getIfPresent(VNU);
        if (cache == null) {
            QueryObjectMetaReq req = new QueryObjectMetaReq();
            req.setUserID(userid);
            req.setVNU(VNU);
            QueryObjectMetaResp resp = QueryObjectMetaHandler.queryObjectMetaCall(req);
            cache = new UploadObjectCache();
            cache.setFilesize(resp.getLength());
            cache.setUserid(userid);
            cache.setBlockNums(resp.getBlocknums());
            uploadObjects.put(VNU, cache);
        }
        return cache;
    }

    public static void delUploadObjectCache(ObjectId VNU) {
        uploadObjects.invalidate(VNU);
    }
}
