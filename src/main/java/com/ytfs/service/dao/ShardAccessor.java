package com.ytfs.service.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class ShardAccessor {

    private static final Logger LOG = Logger.getLogger(ShardAccessor.class);

    public static void saveShardMetas(List<ShardMeta> metas) {
        List<Document> docs = new ArrayList();
        metas.stream().forEach((meta) -> {
            docs.add(meta.toDocument());
        });
        try {
            MongoSource.getShardCollection().insertMany(docs);
        } catch (MongoWriteException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
        /*
         if (MongoSource.getProxy() != null) {
         ShardInsertLog slog = new ShardInsertLog();
         slog.setShards(metas);
         LogMessage log = new LogMessage(Op_Shards_INS, slog);
         MongoSource.getProxy().post(log);
         LOG.debug("DBlog: sync insert shards");
         }*/
    }

    public static ShardMeta[] getShardMeta(long VBI, int shardCount) {
        Long[] VFI = new Long[shardCount];
        for (int ii = 0; ii < shardCount; ii++) {
            VFI[ii] = VBI + ii;
        }
        ShardMeta[] metas = new ShardMeta[shardCount];
        Bson bson = Filters.in("_id", VFI);
        FindIterable<Document> documents = MongoSource.getShardCollection().find(bson).batchSize(shardCount);
        for (Document document : documents) {
            ShardMeta meta = new ShardMeta(document);
            long index = meta.getVFI() - VBI;
            metas[(int) index] = meta;
        }
        return metas;
    }

    public static boolean updateShardMeta(long VFI, int nodeid, byte[] VHF) {
        Bson bson = Filters.eq("_id", VFI);
        Document document = MongoSource.getShardCollection().find(bson).first();
        if (document == null) {
            return false;
        } else {
            ShardMeta meta = new ShardMeta(document);
            if (!Arrays.equals(VHF, meta.getVHF())) {
                return false;
            } else {
                Document doc = new Document("nodeId", nodeid);
                Document update = new Document("$set", doc);
                MongoSource.getShardCollection().updateOne(bson, update);
                return true;
            }
        }
    }
}
