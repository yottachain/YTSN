package com.ytfs.service.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import static com.ytfs.common.ServiceErrorCode.INVALID_NEXTID;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.packet.node.ListDNIResp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class DNIAccessor {

    private static final Logger LOG = Logger.getLogger(DNIAccessor.class);

    public static class LastIterable {

        private List<Document> lastList;
        private FindIterable<Document> iterable = null;
        private boolean exec = false;

        private ListDNIResp query(int nodeId, String nextId, int limit) throws ServiceException {
            ListDNIResp resp = new ListDNIResp();
            if (exec) {
                resp.setNextId(nextId);
                resp.setVhfList(new ArrayList());
                return resp;
            }
            synchronized (this) {
                try {
                    ObjectId nid = null;
                    if (!(nextId == null || nextId.trim().isEmpty())) {
                        try {
                            nid = new ObjectId(nextId);
                        } catch (Exception d) {
                            LOG.warn("Invalid parameter nextId=" + nextId);
                            throw new ServiceException(INVALID_NEXTID);
                        }
                    }
                    if (iterable != null) {
                        for (Document doc : lastList) {
                            ObjectId id = doc.getObjectId("_id");
                            if (id.equals(nid)) {
                                //OK
                            }
                        }
                    }
                    return null;
                } finally {
                    exec = false;
                }
            }
        }
    }

    private static final Cache<Integer, LastIterable> its = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .maximumSize(10000)
            .build();

    public static ListDNIResp listDNI1(int nodeId, String nextId, int limit) throws ServiceException {
        LastIterable lastit = its.getIfPresent(nodeId);
        if (lastit == null) {
            lastit = new LastIterable();
            its.put(nodeId, lastit);
        }
        return lastit.query(nodeId, nextId, limit);
    }

    public static boolean findDNI(int nodeId, byte[] vhf) {
        Bson filter = Filters.eq("minerID", nodeId);
        FindIterable<Document> it = MongoSource.getDNIMetaSource().getDNI_collection().find(filter);
        for (Document doc : it) {
            byte[] bs = ((Binary) doc.get("shard")).getData();
            byte[] VHF = new byte[16];
            System.arraycopy(bs, bs.length - 16, VHF, 0, 16);
            if (Arrays.equals(vhf, VHF)) {
                return true;
            }
        }
        return false;
    }

    public static void UpdateShardNum(List<ShardMeta> ls) {
        Map<Integer, Integer> map = new HashMap();
        ls.forEach((meta) -> {
            Integer num = map.get(meta.getNodeId());
            if (num == null) {
                map.put(meta.getNodeId(), 1);
            } else {
                map.put(meta.getNodeId(), num + 1);
            }
        });
        Set<Map.Entry<Integer, Integer>> set = map.entrySet();
        List<WriteModel<Document>> writeModelList = new ArrayList();
        set.stream().map((ent) -> {
            Bson filter = Filters.in("_id", ent.getKey());
            Document doc = new Document("$inc", new Document("sn" + ServerConfig.superNodeID, ent.getValue()));
            UpdateOneModel update = new UpdateOneModel(filter, doc);
            return update;
        }).forEachOrdered((update) -> {
            writeModelList.add(update);
        });
        BulkWriteOptions option = new BulkWriteOptions();
        MongoSource.getDNIMetaSource().getNode_collection().bulkWrite(writeModelList, option.ordered(false).bypassDocumentValidation(false));
    }

    public static ListDNIResp listDNI(int nodeId, String nextId, int limit) {
        if (limit < 10) {
            limit = 10;
        }
        if (limit > 5000) {
            limit = 5000;
        }
        ObjectId id = null;
        if (nextId != null) {
            id = new ObjectId(nextId);
        }
        Bson filter;
        if (id != null) {
            Bson bson1 = Filters.gt("_id", id);
            Bson bson2 = Filters.eq("minerID", nodeId);
            filter = Filters.and(bson1, bson2);
        } else {
            filter = Filters.eq("minerID", nodeId);
        }
        Document fields = new Document("_id", 1);
        fields.append("shard", 1);
        Bson sort = new Document("_id", 1);
        FindIterable<Document> it = MongoSource.getDNIMetaSource().getDNI_collection().find(filter).projection(fields).sort(sort).limit(limit);
        ListDNIResp resp = new ListDNIResp();
        ObjectId lastObjectId = null;
        it.batchSize(100);
        for (Document doc : it) {
            if (System.currentTimeMillis() - doc.getObjectId("_id").getTimestamp() * 1000L < 1000L * 60L * 5L) {
                break;
            }
            lastObjectId = doc.getObjectId("_id");
            byte[] bs = ((Binary) doc.get("shard")).getData();
            byte[] VHF = new byte[16];
            System.arraycopy(bs, bs.length - 16, VHF, 0, 16);
            resp.addVHF(VHF);
        }
        if (lastObjectId != null) {
            resp.setNextId(lastObjectId.toHexString());
        } else {
            resp.setNextId(nextId);
        }
        return resp;
    }

}
