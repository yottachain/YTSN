package com.ytfs.service.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Filters;
import static com.ytfs.service.packet.ServiceErrorCode.OBJECT_ALREADY_EXISTS;
import com.ytfs.service.packet.ServiceException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileAccessor {

    public static void saveFileMeta(FileMeta meta) throws ServiceException {
        try {
            MongoSource.getFileCollection().insertOne(meta.toDocument());
        } catch (MongoWriteException e) {
            if (e.getMessage().contains("dup key")) {
                throw new ServiceException(OBJECT_ALREADY_EXISTS);
            }
        }
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
