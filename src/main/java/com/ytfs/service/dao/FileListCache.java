package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.lsCacheExpireTime;
import static com.ytfs.common.conf.ServerConfig.lsCacheMaxSize;
import static com.ytfs.common.conf.ServerConfig.lsCachePageNum;
import static com.ytfs.service.dao.FileAccessorV2.firstVersionId;
import com.ytfs.service.packet.s3.ListObjectReq;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.packet.s3.ListObjectRespV2;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import com.ytfs.service.packet.s3.v2.ListObjectReqV2;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileListCache {

    static final Logger LOG = Logger.getLogger(FileListCache.class);

    private static Cache<String, Object> L1_CACHE;

    public synchronized static Cache<String, Object> getL1Cache() {
        if (L1_CACHE == null) {
            L1_CACHE = CacheBuilder.newBuilder()
                    .expireAfterWrite(lsCacheExpireTime, TimeUnit.SECONDS)
                    .maximumSize(lsCacheMaxSize)
                    .build();
            LOG.info("Init FileListCache,lsCacheMaxSize:" + lsCacheMaxSize);
        }
        return L1_CACHE;
    }

    public static void putL1Cache(String key, Object obj) {
        getL1Cache().put(key, obj);
    }

    public static Object getL1Cache(String key) {
        return getL1Cache().getIfPresent(key);
    }

    private final ListObjectReq req;
    private final ListObjectReqV2 reqv2;
    private Object res = null;
    private final String nextFileName;
    private final ObjectId nextVersionId;
    private String prefix;
    private int limit;
    private final boolean compress;

    public Object getResult() {
        return res;
    }

    public FileListCache(ListObjectReqV2 reqv2) {
        this.req = null;
        this.reqv2 = reqv2;
        nextFileName = reqv2.getFileName();
        nextVersionId = reqv2.getNextVersionId();
        prefix = reqv2.getPrefix();
        limit = reqv2.getLimit();
        if (limit < 10) {
            limit = 10;
        }
        if (limit > 1000) {
            limit = 1000;
        }
        compress = reqv2.isCompress();
    }

    public FileListCache(ListObjectReq req) {
        this.req = req;
        this.reqv2 = null;
        nextFileName = req.getFileName();
        nextVersionId = req.getNextVersionId();
        prefix = req.getPrefix();
        limit = req.getLimit();
        if (limit < 10) {
            limit = 10;
        }
        if (limit > 1000) {
            limit = 1000;
        }
        compress = req.isCompress();
    }

    public String getHashCode(int userId, String FileName, ObjectId VersionId) {
        if (reqv2 != null) {
            return reqv2.getHashCode(userId, FileName, VersionId);
        } else {
            return req.getHashCode(userId, FileName, VersionId);
        }
    }

    public void listBucket(ObjectId bucketId, String bucketName, int userId, String key) throws ServiceException {
        long st = System.currentTimeMillis();
        int maxline = limit * lsCachePageNum;
        String lastkey = key;
        Bson filter = null;
        Bson regex = null;
        if (!(prefix == null || prefix.isEmpty())) {
            prefix = prefix.replace("\\", "\\\\");
            regex = Filters.regex("fileName", "^" + prefix);
        }
        if (nextFileName == null || nextFileName.isEmpty()) {
            if (regex == null) {
                filter = Filters.eq("bucketId", bucketId);
            } else {
                filter = Filters.and(Filters.eq("bucketId", bucketId), regex);
            }
        } else {
            Bson bson1 = Filters.eq("bucketId", bucketId);
            Bson bson2 = Filters.eq("fileName", nextFileName);
            Bson bson = Filters.and(bson1, bson2);
            Document fields = new Document("_id", 1);
            Document doc = MongoSource.getFileCollection(userId).find(bson).projection(fields).first();
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
            if (!nextVersionId.equals(firstVersionId)) {
                toFindNextVersionId = true;
            }
        }
        int count = 0;
        int pagenum = 0;
        List<FileMetaV2> list = new ArrayList();
        FindIterable<Document> it = fields == null
                ? MongoSource.getFileCollection(userId).find(filter).sort(sort).limit(maxline)
                : MongoSource.getFileCollection(userId).find(filter).projection(fields).sort(sort).limit(maxline);
        it.batchSize(100);
        MongoCursor<Document> cursor = it.iterator();
        try {
            while (cursor.hasNext() && pagenum < lsCachePageNum) {
                Document doc = cursor.next();
                List ls = (List) doc.get("version");
                int vercount = 0, versize = ls.size();
                for (Object obj : ls) {
                    if (pagenum >= lsCachePageNum) {
                        break;
                    }
                    vercount++;
                    Document verdoc = (Document) obj;
                    if (!toFindNextVersionId) {
                        FileMetaV2 meta = new FileMetaV2(doc, verdoc);
                        meta.setLatest(vercount == versize);
                        list.add(meta);
                        count++;
                        if (count >= limit) {
                            Object obResp = toListObjectResp(list);
                            if (res == null) {
                                res = obResp;
                            }
                            FileListCache.putL1Cache(lastkey, obResp);
                            pagenum++;
                            list = new ArrayList();
                            lastkey = getHashCode(userId, meta.getFileName(), meta.getVersionId());
                            count = 0;
                        }
                    } else {
                        ObjectId verId = verdoc.getObjectId("versionId");
                        if (verId != null && verId.equals(nextVersionId)) {
                            toFindNextVersionId = false;
                        }
                    }
                }
                if (toFindNextVersionId) {
                    throw new ServiceException(INVALID_NEXTVERSIONID);//无效的nextVersionId
                }
            }
        } finally {
            try {
                cursor.close();
            } catch (Exception e) {
            }
        }
        Object obResp = toListObjectResp(list);
        if (res == null) {
            res = obResp;
        }
        if (!list.isEmpty()) {
            FileListCache.putL1Cache(lastkey, obResp);
        }
        LOG.info("LIST object:" + userId + "/" + bucketName + "/" + prefix
                + ",return lines:" + (list.size() + pagenum * limit) + ",take times " + (System.currentTimeMillis() - st) + " ms");
    }

    private Object toListObjectResp(List<FileMetaV2> fileMetaV2s) {
        List<FileMetaMsg> fileMetaMsgs = new ArrayList<>();
        fileMetaV2s.stream().map((fileMetaV2) -> {
            FileMetaMsg fileMetaMsg = new FileMetaMsg();
            fileMetaMsg.setAcl(fileMetaV2.getAcl());
            fileMetaMsg.setBucketId(fileMetaV2.getBucketId());
            fileMetaMsg.setFileId(fileMetaV2.getFileId());
            fileMetaMsg.setFileName(fileMetaV2.getFileName());
            fileMetaMsg.setMeta(fileMetaV2.getMeta());
            fileMetaMsg.setVersionId(fileMetaV2.getVersionId());
            fileMetaMsg.setLatest(fileMetaV2.isLatest());
            return fileMetaMsg;
        }).forEachOrdered((fileMetaMsg) -> {
            fileMetaMsgs.add(fileMetaMsg);
        });
        if (!fileMetaMsgs.isEmpty()) {
            if (this.compress) {
                ListObjectRespV2 resp = new ListObjectRespV2();
                resp.setFileMetaMsgList(fileMetaMsgs);
                return resp;
            } else {
                ListObjectResp resp = new ListObjectResp();
                resp.setFileMetaMsgList(fileMetaMsgs);
                return resp;
            }
        } else {
            ListObjectResp resp = new ListObjectResp();
            resp.setFileMetaMsgList(fileMetaMsgs);
            return resp;
        }
    }
}
