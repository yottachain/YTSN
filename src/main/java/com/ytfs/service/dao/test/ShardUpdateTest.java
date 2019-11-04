package com.ytfs.service.dao.test;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import static com.ytfs.service.dao.test.ShardInsertTest.VHF;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

public class ShardUpdateTest {

    private static final Logger LOG = Logger.getLogger(ShardUpdateTest.class);
    private static Thread t;
    private static ConcurrentLinkedDeque<Long> ls;
    private static Update[] update = new Update[20];
    private static AtomicLong count = new AtomicLong();

    public static void start() {
        t = new Thread() {
            @Override
            public void run() {
                LOG.info("Loader.....");
                ls = ShardTest.lsShardMetas();
                LOG.info("Loader meta,total:" + ls.size());
                long l = System.currentTimeMillis();
                for (int ii = 0; ii < update.length; ii++) {
                    update[ii] = new Update();
                    update[ii].start();
                }
                while (!this.isInterrupted()) {
                    try {
                        sleep(1000 * 15);
                        LOG.info("Update meta total:" + count.get() + ", take times " + (System.currentTimeMillis() - l) + "ms.");
                        if (ls.isEmpty()) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };
        t.start();
    }

    public static void stop() {
        for (int ii = 0; ii < update.length; ii++) {
            update[ii].interrupt();
        }
        t.interrupt();

    }

    private static int[] nodeids = new int[]{121, 125, 129, 130, 134, 138, 140, 143, 144, 159, 164, 116, 167, 180, 188, 191, 195, 197};

    private static List<WriteModel<Document>> makeUpdates() {
        int index = 0;
        List<WriteModel<Document>> requests = new ArrayList<>();
        for (int ii = 0; ii < 160; ii++) {
            Long id = ls.pollFirst();
            if (id == null) {
                break;
            }
            if (index >= nodeids.length) {
                index = 0;
            }
            int nodeid = nodeids[index++];
            Document queryDocument = new Document("_id", id);
            Document updateDocument = new Document("$set", new Document("nodeId", nodeid));
            UpdateOneModel<Document> uom = new UpdateOneModel(queryDocument, updateDocument, new UpdateOptions().upsert(false));
            requests.add(uom);
        }
        return requests;
    }

    private static List<WriteModel<Document>> makeReplaces() {
        int index = 0;
        List<WriteModel<Document>> requests = new ArrayList<>();
        for (int ii = 0; ii < 160; ii++) {
            Long id = ls.pollFirst();
            if (id == null) {
                break;
            }
            if (index >= nodeids.length) {
                index = 0;
            }
            int nodeid = nodeids[index++];
            Document queryDocument = new Document("_id", id);

            Document doc = new Document();
            doc.append("_id", id);
            doc.append("nodeId", nodeid);
            doc.append("VHF", new Binary(VHF));

            ReplaceOneModel<Document> uom = new ReplaceOneModel(queryDocument, doc, new ReplaceOptions().upsert(false));
            requests.add(uom);
        }
        return requests;
    }

    private static class Update extends Thread {

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                try {
                    //List<WriteModel<Document>> ls = makeUpdates();
                    List<WriteModel<Document>> ls = makeReplaces();
                    if (ls.isEmpty()) {
                        break;
                    }
                    ShardTest.updateShardMetas(ls);
                    count.addAndGet(ls.size());
                } catch (Throwable e) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }

        }
    }

}
