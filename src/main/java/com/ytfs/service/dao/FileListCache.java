package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import com.ytfs.common.ServiceException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileListCache {

    static final Logger LOG = Logger.getLogger(FileListCache.class);
    private static final long MAX_SIZE = 100000;
    private static final long READ_EXPIRED_TIME = 3;
    public static final ObjectId firstVersionId = new ObjectId("000000000000000000000000");

    private static final Cache<String, FileListCache> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static List<FileMetaV2> listBucket(String pubkey, int userId, ObjectId bucketId, String nextFileName,
            ObjectId nextVersionId, String prefix, int limit) throws ServiceException {
        FileListCacheKey cachekey = new FileListCacheKey(pubkey, bucketId, prefix, nextVersionId);
        FileListCache newcache = new FileListCache(nextFileName, nextVersionId);
        String key = cachekey.getKey();
        FileListCache oldcache = cache.getIfPresent(key);
        if (oldcache == null) {
            long st = System.currentTimeMillis();
            listBucket(cachekey, newcache, userId, limit);
            LOG.info("List " + key + ",take times " + (System.currentTimeMillis() - st) + " ms.");
            if (newcache.curpos != null) {
                cache.put(key, newcache);
            }
            return newcache.res;
        }
        synchronized (oldcache) {
            if (oldcache.nextFileName.equalsIgnoreCase(newcache.nextFileName)
                    && (newcache.nextVersionId == null
                    || oldcache.nextVersionId.equals(newcache.nextVersionId))) {
                return retFromCache(key, newcache, oldcache, limit);
            }
            boolean ok = false;
            List<FileMetaV2> list = oldcache.res;
            while (!list.isEmpty()) {
                FileMetaV2 m = list.remove(0);
                if (newcache.nextFileName.equalsIgnoreCase(m.getFileName())) {
                    if (newcache.nextVersionId == null || newcache.nextVersionId.equals(m.getVersionId())) {
                        ok = true;
                        break;
                    }
                }
            }
            if (!ok) {
                cache.invalidate(key);
                long st = System.currentTimeMillis();
                listBucket(cachekey, newcache, userId, limit);
                LOG.info("List " + key + ",take times " + (System.currentTimeMillis() - st) + " ms.");
                if (newcache.curpos != null) {
                    cache.put(key, newcache);
                }
                return newcache.res;
            }
            oldcache.nextFileName = newcache.nextFileName;
            oldcache.nextVersionId = newcache.nextVersionId;
            return retFromCache(key, newcache, oldcache, limit);
        }
    }

    public static List<FileMetaV2> retFromCache(String key, FileListCache newcache, FileListCache oldcache, int limit) {
        long st = System.currentTimeMillis();
        List<FileMetaV2> list = new ArrayList();
        int count = 0;
        for (FileMetaV2 m : oldcache.res) {
            list.add(m);
            count++;
            if (count >= limit) {
                LOG.info("List " + key + " from cache,take times " + (System.currentTimeMillis() - st) + " ms.");
                return list;
            }
        }
        if (oldcache.lastDoc != null) {
            List lastDoc = (List) oldcache.lastDoc.get("version");
            List ls = new ArrayList(lastDoc);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoc.remove(obj);
                FileMetaV2 meta = new FileMetaV2(oldcache.lastDoc, verdoc);
                meta.setLatest(vercount == versize);
                list.add(meta);
                count++;
                if (count >= limit) {
                    oldcache.res = list;
                    oldcache.lastDoc.put("version", lastDoc);
                    LOG.info("List " + key + " from lastDoc,take times " + (System.currentTimeMillis() - st) + " ms.");
                    return list;
                }
            }
        }
        oldcache.lastDoc = null;
        MongoCursor<Document> it = oldcache.curpos;
        while (it.hasNext()) {
            Document doc = it.next();
            List lastDoc = (List) doc.get("version");
            List ls = new ArrayList(lastDoc);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoc.remove(obj);
                FileMetaV2 meta = new FileMetaV2(doc, verdoc);
                meta.setLatest(vercount == versize);
                list.add(meta);
                count++;
                if (count >= limit) {
                    oldcache.res = list;
                    doc.put("version", lastDoc);
                    oldcache.lastDoc = doc;
                    LOG.info("List " + key + " from curpos,take times " + (System.currentTimeMillis() - st) + " ms.");
                    return list;
                }
            }
        }
        cache.invalidate(key);
        LOG.info("List " + key + " from cache,curpos closed,take times " + (System.currentTimeMillis() - st) + " ms.");
        return list;
    }

    public static void listBucket(FileListCacheKey cachekey, FileListCache lscache, int userId, int lit) throws ServiceException {
        int limit = lit < 10 ? 10 : lit;
        Bson filter = null;
        Bson regex = null;
        if (!cachekey.prefix.isEmpty()) {
            regex = Filters.regex("fileName", "^" + cachekey.prefix.replace("\\", "\\\\"));
        }
        if (lscache.nextFileName.isEmpty()) {
            if (regex == null) {
                filter = Filters.eq("bucketId", cachekey.bucketId);
            } else {
                filter = Filters.and(Filters.eq("bucketId", cachekey.bucketId), regex);
            }
        } else {
            Bson bson1 = Filters.eq("bucketId", cachekey.bucketId);
            Bson bson2 = Filters.eq("fileName", lscache.nextFileName);
            Bson bson = Filters.and(bson1, bson2);
            Document fields = new Document("_id", 1);
            Document doc = MongoSource.getFileCollection(userId).find(bson).projection(fields).first();
            if (doc == null) {
                throw new ServiceException(INVALID_NEXTFILENAME);//无效的nextFileName
            }
            Bson bson3 = lscache.nextVersionId == null
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
        if (lscache.nextVersionId == null) {
            fields = new Document("_id", 1);
            fields.append("bucketId", 1).append("fileName", 1);
            fields.append("version", new Document("$slice", -1));
        } else {
            if (!lscache.nextVersionId.equals(firstVersionId)) {
                toFindNextVersionId = true;
            }
        }
        int count = 0;
        List<FileMetaV2> ress = new ArrayList();
        FindIterable<Document> it = fields == null
                ? MongoSource.getFileCollection(userId).find(filter).sort(sort)
                : MongoSource.getFileCollection(userId).find(filter).projection(fields).sort(sort);
        MongoCursor<Document> cursor = it.iterator();
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            List lastDoc = (List) doc.get("version");
            List ls = new ArrayList(lastDoc);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoc.remove(obj);
                if (!toFindNextVersionId) {
                    FileMetaV2 meta = new FileMetaV2(doc, verdoc);
                    meta.setLatest(vercount == versize);
                    ress.add(meta);
                    count++;
                } else {
                    ObjectId verId = verdoc.getObjectId("versionId");
                    if (verId != null && verId.equals(lscache.nextVersionId)) {
                        toFindNextVersionId = false;
                    }
                }
                if (count >= limit) {
                    lscache.res = ress;
                    lscache.curpos = cursor;
                    doc.put("version", lastDoc);
                    lscache.lastDoc = doc;
                    return;
                }
            }
            if (toFindNextVersionId) {
                throw new ServiceException(INVALID_NEXTVERSIONID);//无效的nextVersionId
            }
        }
        lscache.res = ress;
        lscache.curpos = null;
    }

    private String nextFileName;
    private ObjectId nextVersionId;
    private List<FileMetaV2> res;
    private MongoCursor<Document> curpos = null;
    private Document lastDoc = null;

    FileListCache(String nextFileName, ObjectId nextVersionId) {
        this.nextFileName = nextFileName == null ? "" : nextFileName.trim();
        this.nextVersionId = nextVersionId;
    }
}
