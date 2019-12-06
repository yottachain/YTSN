package com.ytfs.service.dao;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.common.Function;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.sync.BlockDataLog;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Block_Data;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Block_NLINK_INC;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Block_New;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

public class BlockAccessor {

    private static final Logger LOG = Logger.getLogger(BlockAccessor.class);

    public static void saveBlockMeta(BlockMeta meta) {
        MongoSource.getBlockCollection().insertOne(meta.toDocument());
        if (MongoSource.getProxy() != null) {
            LogMessage log = new LogMessage(Op_Block_New, meta);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync blocks " + meta.getVBI());
        }
    }

    public static void saveBlockData(long vbi, byte[] dat) {
        Document doc = new Document();
        doc.append("_id", vbi);
        doc.append("dat", new Binary(dat));
        try {
            MongoSource.getBlockDatCollection().insertOne(doc);
        } catch (MongoException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
        if (MongoSource.getProxy() != null) {
            BlockDataLog blog = new BlockDataLog();
            blog.setId(vbi);
            blog.setDat(dat);
            LogMessage log = new LogMessage(Op_Block_Data, blog);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync block data " + vbi);
        }
    }

    public static void incBlockNLINK(BlockMeta meta, int num) {
        if (meta.getNLINK() < 0xFFFFFF) {
            Bson filter = Filters.eq("_id", meta.getVBI());
            Document update = new Document("$inc", new Document("NLINK", num));
            MongoSource.getBlockCollection().findOneAndUpdate(filter, update);
            if (MongoSource.getProxy() != null) {
                LogMessage log = new LogMessage(Op_Block_NLINK_INC, Function.long2bytes(meta.getVBI()));
                MongoSource.getProxy().post(log);
                LOG.debug("DBlog: sync block NLINK");
            }
        }
    }

    public static long getUsedSpace(List<Long> ids) {
        long space = 0;
        Bson filter = Filters.in("_id", ids);
        Document fields = new Document("VNF", 1);
        fields.append("AR", 1);
        fields.append("NLINK", 1);
        FindIterable<Document> it = MongoSource.getBlockCollection().find(filter).projection(fields);
        for (Document doc : it) {
            int ar = doc.getInteger("AR");//0 RS  1 多副本 -1数据库
            int vnf = doc.getInteger("VNF");
            long nlink = doc.getLong("NLINK");
            if (ar != -1) {
                space = space + ServerConfig.PFL * vnf / nlink;
            } else {
                space = space + ServerConfig.PCM;
            }
        }
        return space;
    }

    public static BlockMeta getBlockMeta(long VBI) {
        Bson filter = Filters.eq("_id", VBI);
        Document doc = MongoSource.getBlockCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new BlockMeta(doc);
        }
    }

    public static BlockMeta getBlockMetaVNF(long VBI) throws ServiceException {
        Bson filter = Filters.eq("_id", VBI);
        Document fields = new Document("VNF", 1);
        fields.append("AR", 1);
        Document doc = MongoSource.getBlockCollection().find(filter).projection(fields).first();
        if (doc == null) {
            throw new ServiceException(SERVER_ERROR);
        } else {
            return new BlockMeta(doc);
        }
    }

    public static byte[] readBlockData(long vbi) {
        Bson filter = Filters.eq("_id", vbi);
        Document doc = MongoSource.getBlockDatCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return ((Binary) doc.get("dat")).getData();
        }
    }

    public static List<BlockMeta> getBlockMeta(byte[] VHP) {
        List<BlockMeta> ls = new ArrayList();
        Bson filter = Filters.eq("VHP", new Binary(VHP));
        Document fields = new Document("VHB", 1);
        fields.append("KED", 1);
        FindIterable<Document> it = MongoSource.getBlockCollection().find(filter).projection(fields);
        for (Document doc : it) {
            ls.add(new BlockMeta(doc));
        }
        return ls;
    }

    public static BlockMeta getBlockMeta(byte[] VHP, byte[] VHB) {
        Bson bson1 = Filters.eq("VHP", new Binary(VHP));
        Bson bson2 = Filters.eq("VHB", new Binary(VHB));
        Bson bson = Filters.and(bson1, bson2);
        Document fields = new Document("_id", 1);
        fields.append("NLINK", 1);
        fields.append("VNF", 1);
        fields.append("AR", 1);
        fields.append("KED", 1);
        Document doc = MongoSource.getBlockCollection().find(bson).projection(fields).first();
        if (doc == null) {
            return null;
        } else {
            return new BlockMeta(doc);
        }
    }

}
