package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileAccessor {

    public static void saveFileMeta(FileMeta meta) {
        MongoSource.getFileCollection().insertOne(meta.toDocument());
    }

    public static FileMeta getFileMeta(ObjectId bucketid, String filename) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        Document doc = MongoSource.getFileCollection().find(bson).first();
        if (doc == null) {
            return null;
        } else {
            return new FileMeta(doc);
        }
    }
}
