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
import org.bson.types.Binary;

public class BucketAccessor {

    private static final Logger LOG = Logger.getLogger(BucketAccessor.class);
    static final int Max_Bucket_count = 100;

    public static void saveBucketMeta(BucketMeta meta) throws ServiceException {
        long count = MongoSource.getBucketCollection(meta.getUserId()).countDocuments();
        if (count >= Max_Bucket_count) {
            throw new ServiceException(TOO_MANY_BUCKETS);
        }
        try {
            MongoSource.getBucketCollection(meta.getUserId()).insertOne(meta.toDocument());
        } catch (MongoWriteException e) {
            if (e.getMessage().contains("dup key")) {
                throw new ServiceException(BUCKET_ALREADY_EXISTS);
            }
        }
    }

    public static void updateBucketMeta(BucketMeta meta) throws ServiceException {
        Bson bson = Filters.eq("_id", meta.getBucketId());
        Document data = new Document("meta", new Binary(meta.getMeta()));
        Document update = new Document("$set", data);
        MongoSource.getBucketCollection(meta.getUserId()).updateOne(bson, update);
    }

    public static BucketMeta getBucketMeta(int userid, String bucketname) {
        Bson bson = Filters.eq("bucketName", bucketname);
        Document doc = MongoSource.getBucketCollection(userid).find(bson).first();
        if (doc == null) {
            return null;
        } else {
            return new BucketMeta(doc, userid);
        }
    }

    public static String[] listBucket(int userid) {
        List<String> ls = new ArrayList();
        Document fields = new Document("bucketName", 1);
        FindIterable<Document> it = MongoSource.getBucketCollection(userid).find().projection(fields);
        for (Document doc : it) {
            ls.add(doc.getString("bucketName"));
        }
        String[] res = new String[ls.size()];
        return ls.toArray(res);
    }

    public static void deleteBucketMeta(BucketMeta meta) throws ServiceException {
        Bson bson = Filters.eq("_id", meta.getBucketId());
        MongoSource.getBucketCollection(meta.getUserId()).deleteOne(bson);
    }

}
