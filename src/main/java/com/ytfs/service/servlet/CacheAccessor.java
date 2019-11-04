package com.ytfs.service.servlet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.packet.ObjectRefer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class CacheAccessor {

    private static final Logger LOG = Logger.getLogger(CacheAccessor.class);

    private static final long OBJ_MAX_SIZE = 500000;
    private static final long OBJ_EXPIRED_TIME = 3;

    private static final Cache<ObjectId, UploadObjectCache> uploadObjects = CacheBuilder.newBuilder()
            .expireAfterAccess(OBJ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(OBJ_MAX_SIZE)
            .build();

    public static UploadObjectCache getUploadObjectCache(ObjectId VNU) {
        return uploadObjects.getIfPresent(VNU);
    }

    public static UploadObjectCache getUploadObjectCache(int userid, ObjectId VNU) throws ServiceException {
        try {
            UploadObjectCache cache = uploadObjects.get(VNU, () -> {
                ObjectMeta meta = ObjectAccessor.getObject(userid, VNU);
                if (meta != null) {
                    if (meta.getUserID() == userid) {
                        UploadObjectCache ca = new UploadObjectCache();
                        ca.setFilesize(meta.getLength());
                        ca.setUserid(userid);
                        ca.setUsedspace(meta.getUsedspace());
                        List<ObjectRefer> refers = ObjectRefer.parse(meta.getBlockList());
                        refers.stream().forEach((refer) -> {
                            ca.setBlockNum(refer.getId());
                        });
                        return ca;
                    }
                }
                return null;
            });
            if (cache == null) {
                throw new ServiceException(INVALID_UPLOAD_ID);
            } else {
                return cache;
            }
        } catch (ServiceException se) {
            throw se;
        } catch (ExecutionException e) {
            LOG.info("Query object meta err:" + e.getCause().getMessage());
            throw new ServiceException(SERVER_ERROR);
        }
    }

    public static void putUploadObjectCache(ObjectId VNU, UploadObjectCache cache) {
        uploadObjects.put(VNU, cache);
    }

    public static void delUploadObjectCache(ObjectId VNU) {
        uploadObjects.invalidate(VNU);
    }

}
