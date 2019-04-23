package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

public class BucketAccessor {

    public static void saveBucketMeta(BucketMeta meta) {
        MongoSource.getBucketCollection().insertOne(meta.toDocument());
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

    public static int getBucketCount(int userid) {
        //MongoSource.getBucketCollection().aggregate(list)
        return 0;

    }

}
