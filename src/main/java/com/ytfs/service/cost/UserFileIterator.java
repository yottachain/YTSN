package com.ytfs.service.cost;

import com.mongodb.client.FindIterable;
import static com.ytfs.common.Function.bytes2Integer;
import static com.ytfs.common.conf.ServerConfig.PMS;
import static com.ytfs.common.conf.ServerConfig.PPC;
import com.ytfs.service.dao.MongoSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class UserFileIterator {
    
    private static final Logger LOG = Logger.getLogger(UserFileIterator.class);
    private static final long FIRST_CYCLE = PMS * PPC;
    private static final int MAX_IDLIST = 1000;
    private final int userId;
    private long usedSpace = 0;
    private int sumTimes = 0;
    
    public UserFileIterator(int userId) {
        this.userId = userId;
    }
    
    public void iterate() {
        LOG.info("User " + userId + " sum fee...");
        Map<Integer, List<Long>> map = new HashMap();
        Document fields = new Document("blocks", 1);
        fields.append("VNU", 1);
        FindIterable<Document> it = MongoSource.getObjectCollection(userId).find().projection(fields);
        for (Document doc : it) {
            ObjectId id = doc.getObjectId("VNU");
            if (System.currentTimeMillis() - id.getTimestamp() * 1000 < FIRST_CYCLE) {
                continue;
            }
            if (doc.get("blocks") != null) {
                List ls = (List) doc.get("blocks");
                for (Object obj : ls) {
                    byte[] bs = ((Binary) obj).getData();
                    long VBI = bytes2Integer(bs, 0, 8);
                    int superID = bs[8];
                    List<Long> idlist = map.get(superID);
                    if (idlist == null) {
                        idlist = new ArrayList();
                        map.put(superID, idlist);
                    }
                    idlist.add(VBI);
                    if (idlist.size() > MAX_IDLIST) {
                        addSumTimes();
                        UsedSpaceMapper.startUploadShard(idlist, superID, this);
                        map.put(superID, new ArrayList());
                    }
                }
            }
        }
        Set<Map.Entry<Integer, List<Long>>> set = map.entrySet();
        set.stream().filter((ent) -> (ent.getValue() != null && !ent.getValue().isEmpty())).forEachOrdered((ent) -> {
            addSumTimes();
            UsedSpaceMapper.startUploadShard(ent.getValue(), ent.getKey(), this);
        });
        synchronized (this) {
            while (sumTimes > 0) {
                try {
                    this.wait(15000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    private void addSumTimes() {
        synchronized (this) {
            sumTimes++;
        }
    }
    
    public void addUsedSpace(long space) {
        synchronized (this) {
            usedSpace = usedSpace + space;
            sumTimes--;
            this.notify();
        }
    }
    
    public long getUsedSpace() {
        return usedSpace;
    }
}
