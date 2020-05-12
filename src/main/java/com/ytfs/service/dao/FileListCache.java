package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTFILENAME;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTVERSIONID;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import static com.ytfs.common.ServiceErrorCode.TOO_MANY_CURSOR;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.lsCacheExpireTime;
import static com.ytfs.common.conf.ServerConfig.lsCursorLimit;
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

    private static final Map<String, List<FileListCache>> L2cache = new HashMap();
    private static final Thread cleaner = new Thread() {
        @Override
        public void run() {
            for (;;) {
                try {
                    sleep(10000);
                    clear();
                    sleep(10000);
                } catch (InterruptedException t) {
                    break;
                }
            }
        }
    };

    static {
        cleaner.setDaemon(true);
        cleaner.start();
    }

    private static void clear() {
        List<Map.Entry<String, List<FileListCache>>> ls = new ArrayList(L2cache.entrySet());
        for (Map.Entry<String, List<FileListCache>> ent : ls) {
            if (ent.getValue().isEmpty()) {
                synchronized (L2cache) {
                    L2cache.remove(ent.getKey());
                }
            }
        }
        ls = new ArrayList(L2cache.entrySet());
        for (Map.Entry<String, List<FileListCache>> ent : ls) {
            List<FileListCache> caches = ent.getValue();
            List<FileListCache> lists = new ArrayList(caches);
            lists.stream().filter((ca) -> (System.currentTimeMillis() - ca.lastTime > lsCacheExpireTime * 1000)).map((ca) -> {
                ca.curposClose();
                return ca;
            }).forEachOrdered((ca) -> {
                synchronized (caches) {
                    caches.remove(ca);
                }
            });
        }
    }

    private static String getKey(String nextFileName, String prefix, ObjectId nextVersionId) {
        return prefix + "/" + nextFileName + "/" + (nextVersionId != null ? Base58.encode(nextVersionId.toByteArray()) : "");
    }

    public static List<FileMetaV2> listBucket(String pubkey, int userId, ObjectId bucketId, String nextFileName,
            ObjectId nextVersionId, String prefix, int limit) throws ServiceException {
        prefix = prefix == null ? "" : prefix.trim();
        nextFileName = nextFileName == null ? "" : nextFileName.trim();
        String l2key = pubkey + Base58.encode(bucketId.toByteArray());
        List<FileListCache> cachelist;
        synchronized (L2cache) {
            cachelist = L2cache.get(l2key);
            if (cachelist == null) {
                cachelist = new ArrayList();
                L2cache.put(l2key, cachelist);
            }
        }
        String key = getKey(nextFileName, prefix, nextVersionId);
        FileListCache cache = null;
        synchronized (cachelist) {
            for (FileListCache ca : cachelist) {
                if (ca.key.equals(key)) {
                    cache = ca;
                    break;
                }
            }
            if (cache == null) {
                if (cachelist.size() >= lsCursorLimit) {
                    throw new ServiceException(TOO_MANY_CURSOR);
                }
                cache = new FileListCache();
                cache.key = key;
            }
        }
        try {
            List<FileMetaV2> ls = cache.readFileMeta(userId, bucketId, nextFileName, nextVersionId, prefix, limit);
            if (cache.sign == 2) {
                synchronized (cachelist) {
                    cachelist.remove(cache);
                }
            } else {
                synchronized (cachelist) {
                    if (!cachelist.contains(cache)) {
                        cachelist.add(cache);
                    }
                }
                synchronized (L2cache) {
                    if (!L2cache.containsKey(l2key)) {
                        L2cache.put(l2key, cachelist);
                    }
                }
            }
            return ls;
        } catch (Throwable ex) {
            throw ex instanceof ServiceException ? (ServiceException) ex
                    : new ServiceException(SERVER_ERROR, ex.getMessage());

        }
    }

    private List<FileMetaV2> readFileMeta(int userId, ObjectId bucketId, String nextFileName,
            ObjectId nextVersionId, String prefix, int lit) throws Throwable {
        synchronized (this) {
            if (sign == 0) {
                try {
                    List<FileMetaV2> ls = createCache(userId, bucketId, nextFileName, nextVersionId, prefix, lit);
                    if (sign == 2) {
                        if (curpos != null) {
                            curpos.close();
                        }
                    } else {
                        sign = 1;
                    }
                    return ls;
                } catch (Throwable r) {
                    sign = 2;
                    if (curpos != null) {
                        curpos.close();
                    }
                    throw r;
                }
            } else {
                lastTime = System.currentTimeMillis();
                try {
                    List<FileMetaV2> ls = readFromCache(lit, prefix, nextVersionId);
                    if (sign == 2) {
                        if (curpos != null) {
                            curpos.close();
                        }
                    } else {
                        sign = 1;
                    }
                    return ls;
                } catch (Throwable r) {
                    sign = 2;
                    if (curpos != null) {
                        curpos.close();
                    }
                    throw r;
                }
            }
        }
    }

    private List<FileMetaV2> createCache(int userId, ObjectId bucketId, String nextFileName,
            ObjectId nextVersionId, String prefix, int lit) throws ServiceException {
        long st = System.currentTimeMillis();
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
        curpos = it.iterator();
        while (curpos.hasNext()) {
            lastDoc = curpos.next();
            List lastDoclist = (List) lastDoc.get("version");
            List ls = new ArrayList(lastDoclist);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoclist.remove(obj);
                if (!toFindNextVersionId) {
                    FileMetaV2 meta = new FileMetaV2(lastDoc, verdoc);
                    meta.setLatest(vercount == versize);
                    ress.add(meta);
                    count++;
                    if (count >= limit) {
                        lastDoc.put("version", lastDoclist);
                        LOG.info("List " + key + "return count:" + ress.size() + ",take times " + (System.currentTimeMillis() - st) + " ms.");
                        key = getKey(meta.getFileName(), prefix, nextVersionId == null ? null : meta.getVersionId());
                        return ress;
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
        sign = 2;
        LOG.info("List " + key + "return count:" + ress.size() + ",cursor closed,take times " + (System.currentTimeMillis() - st) + " ms.");
        return ress;
    }

    private List<FileMetaV2> readFromCache(int limit, String prefix, ObjectId nextVersionId) {
        long st = System.currentTimeMillis();
        List<FileMetaV2> list = new ArrayList();
        int count = 0;
        if (lastDoc != null) {
            List lastDocList = (List) lastDoc.get("version");
            List ls = new ArrayList(lastDocList);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDocList.remove(obj);
                FileMetaV2 meta = new FileMetaV2(lastDoc, verdoc);
                meta.setLatest(vercount == versize);
                list.add(meta);
                count++;
                if (count >= limit) {
                    lastDoc.put("version", lastDocList);
                    LOG.info("List " + key + "return count:" + list.size() + " from lastDoc,take times " + (System.currentTimeMillis() - st) + " ms.");
                    key = getKey(meta.getFileName(), prefix, nextVersionId == null ? null : meta.getVersionId());
                    return list;
                }
            }
        }
        lastDoc = null;
        MongoCursor<Document> it = curpos;
        while (it.hasNext()) {
            lastDoc = it.next();
            List lastDoclist = (List) lastDoc.get("version");
            List ls = new ArrayList(lastDoclist);
            int vercount = 0, versize = ls.size();
            for (Object obj : ls) {
                vercount++;
                Document verdoc = (Document) obj;
                lastDoclist.remove(obj);
                FileMetaV2 meta = new FileMetaV2(lastDoc, verdoc);
                meta.setLatest(vercount == versize);
                list.add(meta);
                count++;
                if (count >= limit) {
                    lastDoc.put("version", lastDoclist);
                    LOG.info("List " + key + " return count:" + list.size() + " from curpos,take times " + (System.currentTimeMillis() - st) + " ms.");
                    key = getKey(meta.getFileName(), prefix, nextVersionId == null ? null : meta.getVersionId());
                    return list;
                }
            }
        }
        sign = 2;
        LOG.info("List " + key + " return count:" + list.size() + " from curpos,curpos closed,take times " + (System.currentTimeMillis() - st) + " ms.");
        return list;
    }

    private String key;
    private long lastTime = System.currentTimeMillis();
    private MongoCursor<Document> curpos = null;
    private Document lastDoc = null;
    private int sign = 0;

    private void curposClose() {
        synchronized (this) {
            if (curpos != null) {
                curpos.close();
            }
        }
    }
}
