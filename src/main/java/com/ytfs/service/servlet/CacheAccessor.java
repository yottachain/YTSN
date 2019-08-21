package com.ytfs.service.servlet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.service.packet.bp.QueryObjectMetaReq;
import com.ytfs.service.packet.bp.QueryObjectMetaResp;
import com.ytfs.service.servlet.bp.QueryObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.common.ServiceException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class CacheAccessor {
    
    private static final Logger LOG = Logger.getLogger(CacheAccessor.class);
    private static final long OBJ_MAX_SIZE = 500000;
    private static final long OBJ_EXPIRED_TIME = 9;
    
    private static final Cache<ObjectId, UploadObjectCache> uploadObjects = CacheBuilder.newBuilder()
            .expireAfterWrite(OBJ_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(OBJ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(OBJ_MAX_SIZE)
            .build();
    
    public static UploadObjectCache getUploadObjectCache(int userid, ObjectId VNU) throws ServiceException {
        
     /*
        try{
        UploadObjectCache cache = uploadObjects.get(VNU, new Callable() {
            @Override
            public Object call() throws Exception {
                  return null;
            }
        });
        
        */
        
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
    
    private static final long BLK_MAX_SIZE = 500000;
    private static final long BLK_EXPIRED_TIME = 3;
    private static final Cache<Long, UploadBlockCache> uploadBlocks = CacheBuilder.newBuilder()
            .expireAfterWrite(BLK_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(BLK_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(BLK_MAX_SIZE)
            .build();
    
    public static void addUploadBlockCache(long VBI, UploadBlockCache cache) {
        uploadBlocks.put(VBI, cache);
    }
    
    public static UploadBlockCache getUploadBlockCache(long VBI) throws ServiceException {
        UploadBlockCache cache = uploadBlocks.getIfPresent(VBI);
        if (cache == null) {
            throw new ServiceException(INVALID_UPLOAD_ID,"Block cache invalid:" + VBI);
        }
        return cache;
    }
    
    public static void delUploadBlockCache(long VBI) {
        uploadBlocks.invalidate(VBI);
    }
    
}
