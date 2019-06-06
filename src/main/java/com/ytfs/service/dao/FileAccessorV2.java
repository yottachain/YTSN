package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import com.ytfs.common.ServiceException;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileAccessorV2 {

    /**
     * 插入文件meta,如果记录存在生成新版本
     *
     * @param meta
     * @return 最新版本号
     */
    public static ObjectId saveFileMeta(FileMetaV2 meta) {
        Bson bson1 = Filters.eq("bucketId", meta.getBucketId());
        Bson bson2 = Filters.eq("fileName", meta.getFileName());
        Bson filter = Filters.and(bson1, bson2);
        Document doc = new Document("bucketId", meta.getBucketId());
        doc.append("fileName", meta.getFileName());
        Document update = new Document("$set", doc);
        update.append("$addToSet", new Document("version", meta.getVerDocument()));
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        MongoSource.getFileCollection().updateOne(filter, update, updateOptions);
        return meta.getVersionId();
    }

    /**
     * 根据bucketid,版本号,文件名查询指定版本meta
     *
     * @param bucketid
     * @param filename
     * @param versionId
     * @return FileMetaV2
     */
    public static FileMetaV2 getFileMeta(ObjectId bucketid, String filename, ObjectId versionId) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson3 = Filters.eq("version.versionId", versionId);
        Bson bson = Filters.and(bson1, bson2, bson3);
        Document fields = new Document("_id", 1);
        fields.append("version.$", 1);
        Document doc = MongoSource.getFileCollection().find(bson).projection(fields).first();
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
     * @param bucketid
     * @param filename
     * @return FileMetaV2
     */
    public static FileMetaV2 getFileMeta(ObjectId bucketid, String filename) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        Document fields = new Document("_id", 1);
        fields.append("version", new Document("$slice", -1));
        Document doc = MongoSource.getFileCollection().find(bson).projection(fields).first();
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
     * 统计bucket下文件总数
     *
     * @param bucketId
     * @return long
     */
    public static long getObjectCount(ObjectId bucketId) {
        Bson filter = Filters.eq("bucketId", bucketId);
        return MongoSource.getFileCollection().countDocuments(filter);
    }

    /**
     * 删除文件meta
     *
     * @param bucketid
     * @param filename
     */
    public static void deleteFileMeta(ObjectId bucketid, String filename) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        MongoSource.getFileCollection().deleteOne(bson);
    }

    /**
     * 删除指定版本文件meta
     *
     * @param bucketid
     * @param filename
     * @param versionId
     */
    public static void deleteFileMeta(ObjectId bucketid, String filename, ObjectId versionId) {
        Bson bson1 = Filters.eq("bucketId", bucketid);
        Bson bson2 = Filters.eq("fileName", filename);
        Bson bson = Filters.and(bson1, bson2);
        Document update = new Document("$pull", new Document("version", new Document("versionId", versionId)));
        MongoSource.getFileCollection().updateOne(bson, update);
        Document bson3 = new Document("version", new Document("$size", 0));
        MongoSource.getFileCollection().deleteOne(Filters.and(bson1, bson2, bson3));
    }

    /**
     * 遍历目录
     *
     * @param bucketId
     * @param nextFileName =null,从第一条记录开始
     * @param nextVersionId =null,遍历最新版本,!=null遍历所有版本
     * @param prefix 根据前缀查询
     * @param limit
     * @return　List
     * @throws com.ytfs.common.ServiceException
     */
    public static List<FileMetaV2> listBucket(ObjectId bucketId, String nextFileName, ObjectId nextVersionId, String prefix, int limit) throws ServiceException {
        if (limit < 10) {
            limit = 10;
        }
        Bson filter = null;
        Bson regex = null;
        if (!(prefix == null || prefix.isEmpty())) {
            prefix = prefix.replace("/", "\\/").replace("\\", "\\\\");
            regex = Filters.regex("fileName", "^" + prefix);
        }
        if (nextFileName == null || nextFileName.isEmpty()) {
            if (regex == null) {
                filter = Filters.eq("bucketId", bucketId);
            } else {
                Filters.and(Filters.eq("bucketId", bucketId), regex);
            }
        } else {
            Bson bson1 = Filters.eq("bucketId", bucketId);
            Bson bson2 = Filters.eq("fileName", nextFileName);
            Bson bson = Filters.and(bson1, bson2);
            Document fields = new Document("_id", 1);
            Document doc = MongoSource.getFileCollection().find(bson).projection(fields).first();
            if (doc == null) {
                throw new ServiceException(INVALID_NEXTFILENAME);//无效的nextFileName
            }
            Bson bson3 = nextVersionId == null
                    ? Filters.gt("_id", doc.getObjectId("_id"))
                    : Filters.gte("_id", doc.getObjectId("_id"));
            if (regex == null) {
                filter = Filters.and(bson1, bson3);
            } else {
                filter = Filters.and(bson1, bson3, regex);
            }
        }
        Bson sort = new Document("_id", 1);
        Document fields = null;
        boolean toFindNextVersionId = false;
        if (nextVersionId == null) {
            fields = new Document("_id", 1);
            fields.append("bucketId", 1).append("fileName", 1);
            fields.append("version", new Document("$slice", -1));
        } else {
            toFindNextVersionId = true;
        }
        int count = 0;
        List<FileMetaV2> res = new ArrayList();
        FindIterable<Document> it = fields == null
                ? MongoSource.getFileCollection().find(filter).sort(sort).limit(limit)
                : MongoSource.getFileCollection().find(filter).projection(fields).sort(sort).limit(limit);
        for (Document doc : it) {
            List ls = (List) doc.get("version");
            for (Object obj : ls) {
                Document verdoc = (Document) obj;
                if (!toFindNextVersionId) {
                    FileMetaV2 meta = new FileMetaV2(doc, verdoc);
                    res.add(meta);
                    count++;
                } else {
                    ObjectId verId = verdoc.getObjectId("versionId");
                    if (verId != null && verId.equals(nextVersionId)) {
                        toFindNextVersionId = false;
                    }
                }
                if (count >= limit) {
                    return res;
                }
            }
            if (toFindNextVersionId) {
                throw new ServiceException(INVALID_NEXTVERSIONID);//无效的nextVersionId
            }
        }
        return res;
    }

    public static List<FileMetaV2> listBucket(ObjectId bucketId) throws ServiceException {
        return listBucket(bucketId, null, null, null, 100);
    }

    public static List<FileMetaV2> listBucket(ObjectId bucketId, String nextFileName) throws ServiceException {
        return listBucket(bucketId, nextFileName, null, null, 100);
    }

    public static List<FileMetaV2> listBucket(ObjectId bucketId, String nextFileName, int limit) throws ServiceException {
        return listBucket(bucketId, nextFileName, null, null, limit);
    }

    public static List<FileMetaV2> listBucket(ObjectId bucketId, String nextFileName, String prefix, int limit) throws ServiceException {
        return listBucket(bucketId, nextFileName, null, prefix, limit);
    }
}
