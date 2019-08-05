package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Shards_INS;
import com.ytfs.service.dao.sync.ShardInsertLog;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class ShardAccessor {

    private static final Logger LOG = Logger.getLogger(ShardAccessor.class);

    public static void saveShardMetas(List<ShardMeta> metas) {
        List<Document> docs = new ArrayList();
        for (ShardMeta meta : metas) {
            docs.add(meta.toDocument());
        }
        MongoSource.getShardCollection().insertMany(docs);
        if (MongoSource.getProxy() != null) {
            ShardInsertLog slog = new ShardInsertLog();
            slog.setShards(metas);
            LogMessage log = new LogMessage(Op_Shards_INS, slog);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync insert shards");
        }
    }

    public static ShardMeta[] getShardMeta(long VBI, int shardCount) {
        Long[] VFI = new Long[shardCount];
        for (int ii = 0; ii < shardCount; ii++) {
            VFI[ii] = VBI + ii;
        }
        ShardMeta[] metas = new ShardMeta[shardCount];
        Bson bson = Filters.in("_id", VFI);
        FindIterable<Document> documents = MongoSource.getShardCollection().find(bson).batchSize(shardCount);
        int count = 0;
        for (Document document : documents) {
            metas[count++] = new ShardMeta(document);
        }
        return metas;
    }
}
