package com.ytfs.service.dao.test;

import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.ytfs.service.dao.MongoSource;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.log4j.Logger;
import org.bson.Document;

public class ShardTest {

    private static final String ShardTestTab = "shardtest";
    private static MongoCollection<Document> collection = null;
    private static final Logger LOG = Logger.getLogger(ShardTest.class);

    public static MongoCollection<Document> getCollection() {
        if (collection == null) {
            collection = MongoSource.getCollection(ShardTestTab);
        }
        return collection;
    }

    public static void saveShardMetas(List<Document> metas) {
        getCollection().insertMany(metas);
        try {
            getCollection().insertMany(metas);
        } catch (MongoException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
    }

    public static ConcurrentLinkedDeque<Long> lsShardMetas() {
        List<Long> ls = new LinkedList();
        Document fields = new Document("_id", 1);
        FindIterable<Document> it = getCollection().find().projection(fields);
        int count = 0;
        for (Document doc : it) {
            ls.add(doc.getLong("_id"));
            count++;
            if (count % 1000000 == 0) {
                LOG.info("Load meta count:" + count);
            }
            if (count > 20000000) {
                break;
            }
        }
        Collections.shuffle(ls);
        ConcurrentLinkedDeque<Long> queue = new ConcurrentLinkedDeque(ls);
        return queue;
    }

    public static void updateShardMetas(List<WriteModel<Document>> ls) {
        try {
            BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
            bulkWriteOptions.ordered(false);
            bulkWriteOptions.bypassDocumentValidation(true);
            BulkWriteResult bulkWriteResult = getCollection().bulkWrite(ls,bulkWriteOptions);
        } catch (MongoException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
    }

}
