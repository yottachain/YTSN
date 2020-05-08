package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.lsCacheExpireTime;
import static com.ytfs.service.dao.FileAccessorV2.firstVersionId;
import io.jafka.jeos.util.Base58;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class FileListCache {

    static final Logger LOG = Logger.getLogger(FileListCache.class);
    private static final long MAX_SIZE = 50000;
    private static final long READ_EXPIRED_TIME = 1;

    private static final Cache<String, Object> L1cache = CacheBuilder.newBuilder()
            .expireAfterWrite(lsCacheExpireTime, TimeUnit.SECONDS)
            .maximumSize(MAX_SIZE)
            .build();

    public static void putL1Cache(String key, Object obj) {
        L1cache.put(key, obj);
    }

    public static Object getL1Cache(String key) {
        return L1cache.getIfPresent(key);
    }

    private static final Map<String, Cache<String, FileListCache>> L2cache = new HashMap();

    private static String GetKey(ObjectId nextFileId, String prefix, ObjectId nextVersionId) {
        return nextFileId == null ? "/" : (Base58.encode(nextFileId.toByteArray()) + "/")
                + prefix + "/" + (nextVersionId != null ? "1" : "");
    }

    public static List<FileMetaV2> listBucket1(String pubkey, int userId, ObjectId bucketId, ObjectId nextFileId,
            ObjectId nextVersionId, String prefix, int limit) throws ServiceException {
        prefix = prefix == null ? "" : prefix.trim();
        String l2key = pubkey + Base58.encode(bucketId.toByteArray());
        Cache<String, FileListCache> map;
        synchronized (L2cache) {
            map = L2cache.get(l2key);
            if (map == null) {
                map = CacheBuilder.newBuilder()
                        .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
                        .maximumSize(MAX_SIZE)
                        .removalListener(new BucketRemovalListener(l2key))
                        .build();
                L2cache.put(l2key, map);
            }
        }

        String key = GetKey(nextFileId, prefix, nextVersionId);
        
        /*
        FileListCache cache = null;
      
        if (cache == null) {
            if (map.size() > 3) {
                throw new ServiceException(TOO_MANY_CURSOR);
            }
            long st = System.currentTimeMillis();
            cache = createCache(userId, bucketId, nextFileName, nextVersionId, prefix, limit);
            if (cache.curpos == null) {
                LOG.info("List " + logKey + "return count:" + cache.res.size() + ",cursor closed,take times " + (System.currentTimeMillis() - st) + " ms.");
                return cache.res;
            } else {
                key = GetKey(cache.lastDoc.getString("fileName"), prefix, nextVersionId);
                LOG.info("List " + key + "return count:" + cache.res.size() + ",take times " + (System.currentTimeMillis() - st) + " ms.");
                map.put(key, cache);
                List<FileMetaV2> result = cache.res;
                cache.res = null;
                return result;
            }
        }
        retFromCache(logKey, cache, limit);
        if (cache.curpos == null) {
            map.invalidate(key);
            return cache.res;
        } else {
            String newkey = GetKey(cache.lastDoc.getString("fileName"), prefix, nextVersionId);
            map.put(newkey, cache);
            map.invalidate(key);
            return cache.res;
        }*/
        return null;
    }

    public static FileListCache createCache(int userId, ObjectId bucketId, String nextFileName,
            ObjectId nextVersionId, String prefix, int lit) throws ServiceException {
        int limit = lit < 10 ? 10 : lit;
        Bson filter = null;
        Bson regex = null;
        if (!prefix.isEmpty()) {
            regex = Filters.regex("fileName", "^" + prefix.replace("\\", "\\\\"));
        }
        if (nextFileName.isEmpty()) {
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
                    if (verId != null && verId.equals(nextVersionId)) {
                        toFindNextVersionId = false;
                    }
                }
                if (count >= limit) {
                    FileListCache lscache = new FileListCache();
                    lscache.res = ress;
                    lscache.curpos = cursor;
                    doc.put("version", lastDoc);
                    lscache.lastDoc = doc;
                    return lscache;
                }
            }
            if (toFindNextVersionId) {
                throw new ServiceException(INVALID_NEXTVERSIONID);//无效的nextVersionId
            }
        }
        FileListCache lscache = new FileListCache();
        lscache.res = ress;
        lscache.curpos = null;
        return lscache;
    }

    public static void retFromCache(String logKey, FileListCache cache, int limit) {
        long st = System.currentTimeMillis();
        List<FileMetaV2> list = new ArrayList();
        int count = 0;
        if (cache.lastDoc != null) {
            List lastDoc = (List) cache.lastDoc.get("version");
            List ls = new ArrayList(lastDoc);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoc.remove(obj);
                FileMetaV2 meta = new FileMetaV2(cache.lastDoc, verdoc);
                meta.setLatest(vercount == versize);
                list.add(meta);
                count++;
                if (count >= limit) {
                    cache.res = list;
                    cache.lastDoc.put("version", lastDoc);
                    LOG.info("List " + logKey + "return count:" + cache.res.size() + " from lastDoc,take times " + (System.currentTimeMillis() - st) + " ms.");
                    return;
                }
            }
        }
        cache.lastDoc = null;
        MongoCursor<Document> it = cache.curpos;
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
                    cache.res = list;
                    doc.put("version", lastDoc);
                    cache.lastDoc = doc;
                    LOG.info("List " + logKey + "return count:" + cache.res.size() + " from curpos,take times " + (System.currentTimeMillis() - st) + " ms.");
                    return;
                }
            }
        }
        cache.res = list;
        cache.curpos = null;
        LOG.info("List " + logKey + " from curpos,curpos closed,take times " + (System.currentTimeMillis() - st) + " ms.");
    }

    private List<FileMetaV2> res;
    private MongoCursor<Document> curpos = null;
    private Document lastDoc = null;

    private void curposClose() {
        if (curpos != null) {
            curpos.close();
        }
    }

    private static class BucketRemovalListener implements RemovalListener<String, FileListCache> {

        String key;

        private BucketRemovalListener(String key) {
            this.key = key;
        }

        @Override
        public void onRemoval(RemovalNotification<String, FileListCache> rn) {
            FileListCache ca = rn.getValue();
            ca.curposClose();
            synchronized (L2cache) {
                Cache<String, FileListCache> map = L2cache.get(key);
                if (map != null && map.size() == 0) {
                    L2cache.remove(key);
                }
            }
        }
    }
}
