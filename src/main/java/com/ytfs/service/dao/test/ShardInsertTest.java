package com.ytfs.service.dao.test;

import com.ytfs.service.dao.Sequence;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

public class ShardInsertTest {

    private static final Logger LOG = Logger.getLogger(ShardInsertTest.class);

    private static int[] nodeids = new int[]{21, 25, 29, 30, 34, 38, 40, 43, 44, 59, 64, 66, 67, 80, 88, 91, 95, 97};
    private static byte[] VHF = "1234567890abcdefghijkhigkyajdk3t".getBytes();
    private static AtomicLong count = new AtomicLong();
    private static Thread t;
    private static Insert[] insert = new Insert[10];

    public static void start() {
        for (int ii = 0; ii < insert.length; ii++) {
            insert[ii] = new Insert();
            insert[ii].start();
        }
        t = new Thread() {
            public void run() {
                long l=System.currentTimeMillis();
                while (!this.isInterrupted()) {
                    try {
                        sleep(1000 * 15);
                        LOG.info("Insert meta total:" + count.get()+", take times "+(System.currentTimeMillis()-l)+ "ms.");
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };
        t.start();
    }

    public static void stop() {
        for (int ii = 0; ii < insert.length; ii++) {
            insert[ii].interrupt();
        }
        t.interrupt();
    }

    private static List<Document> makeShards() {
        long VBI = Sequence.generateBlockID(160);
        List<Document> docs = new ArrayList();
        int index = 0;
        for (int ii = 0; ii < 160; ii++) {
            Document doc = new Document();
            doc.append("_id", VBI + ii);
            if (index >= nodeids.length) {
                index = 0;
            }
            doc.append("nodeId", nodeids[index++]);
            doc.append("VHF", new Binary(VHF));
            docs.add(doc);
        }
        return docs;
    }

    private static class Insert extends Thread {

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                try {
                    List<Document> ls = makeShards();
                    ShardTest.saveShardMetas(ls);
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
