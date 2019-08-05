package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import com.ytfs.common.Function;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class Sequence {

    private static final Logger LOG = Logger.getLogger(Sequence.class);
    private static AtomicInteger USERID_SEQ;
    private static final AtomicInteger BLKID_SEQ = new AtomicInteger(0);
    private static int SN_COUNT;

    /**
     * 查找最大的UserID
     */
    public static void initUserID_seq() {
        SN_COUNT = SuperNodeList.getSuperNodeCount();
        Bson filter = Filters.mod("_id", SN_COUNT, ServerConfig.superNodeID);
        Bson sort = new Document("_id", -1);
        Document fields = new Document("_id", 1);
        Document doc = MongoSource.getUserCollection().find(filter).projection(fields).sort(sort).limit(1).first();
        if (doc == null) {
            USERID_SEQ = new AtomicInteger(ServerConfig.superNodeID);
            LOG.info("User sequence init value:" + ServerConfig.superNodeID);
        } else {
            int curid = doc.getInteger("_id");
            USERID_SEQ = new AtomicInteger(curid);
            LOG.info("User sequence init value:" + curid);
        }
    }

    /**
     * 生成一个唯一UserID序列号
     *
     * @return int
     */
    public static int generateUserID() {
        return USERID_SEQ.addAndGet(SN_COUNT);
    }

    /**
     * 生成一个BlockID序列号的低32位
     *
     * @param inc
     * @return
     */
    private static int getSequence(int inc) {
        int seq = BLKID_SEQ.getAndAdd(inc);
        byte[] bs = Function.int2bytes(seq);
        bs[0] = (byte) ServerConfig.superNodeID;
        return Function.bytes2int(bs);
    }

    /**
     * 生成数据块,分片ID
     *
     * @param shardCount
     * @return INT64
     */
    public static long generateBlockID(int shardCount) {
        int h = (int) (System.currentTimeMillis() / 1000);
        int l = getSequence(shardCount);
        long high = (h & 0x000000ffffffffL) << 32;  //高32位
        long low = (++l) & 0x00000000ffffffffL;
        return high | low;
    }
}
