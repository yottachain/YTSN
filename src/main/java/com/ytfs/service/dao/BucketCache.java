package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class BucketCache {

    private static final long MAX_SIZE = 100000;
    private static final long READ_EXPIRED_TIME = 10;

    private static final Cache<String, ObjectId> buckets = CacheBuilder.newBuilder()
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static BucketMeta getBucket(int userid, String bucketname,byte[] byte_meta) {
        String key = userid + bucketname;
        ObjectId id = buckets.getIfPresent(key);
        if (id == null) {
            BucketMeta meta = BucketAccessor.getBucketMeta(userid, bucketname);
            if (meta != null) {
                buckets.put(key, meta.getBucketId());
                return meta;
            }else{
                return null;
            }
        }
        return new BucketMeta(userid, id, bucketname,byte_meta);
    }
}
