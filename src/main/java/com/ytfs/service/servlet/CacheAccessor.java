package com.ytfs.service.servlet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.QueryObjectMetaResp;
import com.ytfs.service.servlet.bp.QueryObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class CacheAccessor {

    private static final long MAX_SIZE = 100000;
    private static final long EXPIRED_TIME = 3;

    private static final Cache<ObjectId, UploadObjectCache> uploadObjects = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
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

    private static final Cache<Long, UploadBlockCache> uploadBlocks = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static void addUploadBlockCache(long VBI, UploadBlockCache cache) {
        uploadBlocks.put(VBI, cache);
    }

    public static UploadBlockCache getUploadBlockCache(long VBI) throws ServiceException {
        UploadBlockCache cache = uploadBlocks.getIfPresent(VBI);
        if (cache == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        return cache;
    }

    public static void delUploadBlockCache(long VBI) {
        uploadBlocks.invalidate(VBI);
    }

}
