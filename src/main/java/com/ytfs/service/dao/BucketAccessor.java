package com.ytfs.service.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.common.ServiceException;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.ytfs.common.ServiceErrorCode.BUCKET_ALREADY_EXISTS;
import static com.ytfs.common.ServiceErrorCode.TOO_MANY_BUCKETS;

public class BucketAccessor {
    private static final Logger LOG = Logger.getLogger(BucketAccessor.class);
    static final int Max_Bucket_count = 100;

    public static void saveBucketMeta(BucketMeta meta) throws ServiceException {
        Bson filter = Filters.eq("userId", meta.getUserId());
        long count = MongoSource.getBucketCollection().countDocuments(filter);
        if (count >= Max_Bucket_count) {
            throw new ServiceException(TOO_MANY_BUCKETS);
        }
        try {
            MongoSource.getBucketCollection().insertOne(meta.toDocument());
        } catch (MongoWriteException e) {
            if (e.getMessage().contains("dup key")) {
                throw new ServiceException(BUCKET_ALREADY_EXISTS);
            }
        }
    }

    public static void updateBucketMeta(BucketMeta meta) throws ServiceException {

        Bson bson1 = Filters.eq("_id", meta.getBucketId());
        Bson bson2 = Filters.eq("userId", meta.getUserId());
        Bson filter = Filters.and(bson1, bson2);
        LOG.info("bucketId======"+meta.getBucketId());

        Document data = meta.toDocument();
        Document update = new Document("$set",data);
        MongoSource.getBucketCollection().updateOne(filter,update);
    }

    public static BucketMeta getBucketMeta(int userid, String bucketname) {
        Bson bson1 = Filters.eq("bucketName", bucketname);
        Bson bson2 = Filters.eq("userId", userid);
        Bson bson = Filters.and(bson1, bson2);
        Document doc = MongoSource.getBucketCollection().find(bson).first();
        if (doc == null) {
            return null;
        } else {
            return new BucketMeta(doc);
        }
    }

    public static String[] listBucket(int userid) {
        List<String> ls = new ArrayList();
        Bson filter = Filters.eq("userId", userid);
        FindIterable<Document> it = MongoSource.getBucketCollection().find(filter);
        for (Document doc : it) {
            ls.add(doc.getString("bucketName"));
        }
        String[] res = new String[ls.size()];
        return ls.toArray(res);
    }

    public static void deleteBucketMeta(BucketMeta meta) throws ServiceException {
        LOG.info("bucketName=====deleteBucketMeta() ======"+meta.getBucketName());
        LOG.info("bucketId=====deleteBucketMeta() ======"+meta.getBucketId());
        Bson bson = Filters.eq("_id", meta.getBucketId());
        //根据bucketId删除bucket
        MongoSource.getBucketCollection().deleteOne(bson);
    }

}
