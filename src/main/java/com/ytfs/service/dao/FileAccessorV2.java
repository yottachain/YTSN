package com.ytfs.service.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import static com.ytfs.common.ServiceErrorCode.OBJECT_ALREADY_EXISTS;
import com.ytfs.common.ServiceException;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileAccessorV2 {

    /**
     * 插入文件meta,如果记录存在抛出OBJECT_ALREADY_EXISTS错误
     *
     * @param userId
     * @param meta
     * @return 版本号
     * @throws ServiceException
     */
    public static ObjectId insertFileMeta(int userId, FileMetaV2 meta) throws ServiceException {
        Document doc = new Document("bucketId", meta.getBucketId());
        doc.append("fileName", meta.getFileName());
        List<Document> vers = new ArrayList();
        vers.add(meta.getVerDocument());
        doc.append("version", vers);
        try {
            MongoSource.getFileCollection(userId).insertOne(doc);
        } catch (MongoWriteException e) {
            if (e.getMessage().contains("dup key")) {
                throw new ServiceException(OBJECT_ALREADY_EXISTS);
            }
        }
        return meta.getVersionId();
    }

    /**
     * 插入文件meta,如果记录存在生成新版本
     *
     * @param userId
     * @param meta
     * @return 最新版本号
     */
    public static ObjectId saveFileMeta(int userId, FileMetaV2 meta) {
        Bson bson1 = Filters.eq("bucketId", meta.getBucketId());
        Bson bson2 = Filters.eq("fileName", meta.getFileName());
        Bson filter = Filters.and(bson1, bson2);
        Document doc = new Document("bucketId", meta.getBucketId());
        doc.append("fileName", meta.getFileName());
        Document update = new Document("$set", doc);
        update.append("$addToSet", new Document("version", meta.getVerDocument()));
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        MongoSource.getFileCollection(userId).updateOne(filter, update, updateOptions);
        return meta.getVersionId();
    }

    /**
     * 根据bucketid,版本号,文件名查询指定版本meta
     *
     * @param userId
     * @param bucketid
     * @param filename
     * @param versionId
     * @return FileMetaV2
     */
    public static FileMetaV2 getFileMeta(int userId, ObjectId bucketid, String filename, ObjectId versionId) {
        if (versionId == null) {
            return getFileMeta(userId, bucketid, filename);
        }
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson3 = Filters.eq("version.versionId", versionId);
        Bson bson = Filters.and(bson1, bson2, bson3);
        Document fields = new Document("_id", 1);
        fields.append("version.$", 1);
        Document doc = MongoSource.getFileCollection(userId).find(bson).projection(fields).first();
        if (doc == null) {
            return null;
        } else {
            FileMetaV2 res = new FileMetaV2(doc);
            res.setBucketId(bucketid);
            res.setFileName(filename);
            return res;
        }
    }

    /**
     * 根据bucketid,文件名查询最新版本meta
     *
     * @param userId
     * @param bucketid
     * @param filename
     * @return FileMetaV2
     */
    public static FileMetaV2 getFileMeta(int userId, ObjectId bucketid, String filename) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        Document fields = new Document("_id", 1);
        fields.append("version", new Document("$slice", -1));
        Document doc = MongoSource.getFileCollection(userId).find(bson).projection(fields).first();
        if (doc == null) {
            return null;
        } else {
            FileMetaV2 res = new FileMetaV2(doc);
            res.setBucketId(bucketid);
            res.setFileName(filename);
            res.setLatest(true);
            return res;
        }
    }

    /**
     * 统计bucket下文件总数
     *
     * @param userId
     * @param bucketId
     * @return long
     */
    public static long getObjectCount(int userId, ObjectId bucketId) {
        Bson filter = Filters.eq("bucketId", bucketId);
        return MongoSource.getFileCollection(userId).countDocuments(filter);
    }

    /**
     * 删除文件meta
     *
     * @param userId
     * @param bucketid
     * @param filename
     */
    public static void deleteFileMeta(int userId, ObjectId bucketid, String filename) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        MongoSource.getFileCollection(userId).deleteOne(bson);
    }

    /**
     * 删除指定版本文件meta
     *
     * @param userId
     * @param bucketid
     * @param filename
     * @param versionId
     */
    public static void deleteFileMeta(int userId, ObjectId bucketid, String filename, ObjectId versionId) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        Document update = new Document("$pull", new Document("version", new Document("versionId", versionId)));
        MongoSource.getFileCollection(userId).updateOne(bson, update);
        Document bson3 = new Document("version", new Document("$size", 0));
        MongoSource.getFileCollection(userId).deleteOne(Filters.and(bson1, bson2, bson3));
    }

 
}
