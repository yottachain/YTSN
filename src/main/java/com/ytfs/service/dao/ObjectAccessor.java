package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_Block_Update;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_NLINK_INC;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_New;
import com.ytfs.service.dao.sync.ObjectMetaLog;
import com.ytfs.service.dao.sync.ObjectUpdateLog;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class ObjectAccessor {

    private static final Logger LOG = Logger.getLogger(ObjectAccessor.class);

    public static void insertOrUpdate(ObjectMeta meta) {
        Bson filter = Filters.eq("VNU", meta.getVNU());
        Document fields = new Document("length", 1);
        Document doc = MongoSource.getObjectCollection().find(filter).projection(fields).first();
        if (doc == null) {
            MongoSource.getObjectCollection().insertOne(meta.toDocument());
        } else {
            if (doc.getLong("length") != meta.getLength()) {
                Bson filter1 = Filters.eq("VNU", meta.getVNU());
                Document data = new Document("length", meta.getLength());
                Document update = new Document("$set", data);
                MongoSource.getObjectCollection().updateOne(filter1, update);
            }
        }
        if (MongoSource.getProxy() != null) {
            ObjectMetaLog metalog = new ObjectMetaLog(meta);
            LogMessage log = new LogMessage(Op_Object_New, metalog);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync object " + metalog.getVNU());
        }
    }

    public static void incObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("$inc", new Document("NLINK", 1));
        MongoSource.getObjectCollection().updateOne(filter, update);
        if (MongoSource.getProxy() != null) {
            LogMessage log = new LogMessage(Op_Object_NLINK_INC, meta.getId());
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync object NLINK");
        }
    }

    public static void getObjectAndUpdateNLINK(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document data = new Document("NLINK", 1);
        Document update = new Document("$set", data);
        Document doc = MongoSource.getObjectCollection().findOneAndUpdate(filter, update);
        if (doc != null) {
            meta.fill(doc);
            if (MongoSource.getProxy() != null) {
                LogMessage log = new LogMessage(Op_Object_NLINK_INC, meta.getId());
                MongoSource.getProxy().post(log);
                LOG.debug("DBlog: sync object NLINK");
            }
        }
    }

    public static void updateObject(ObjectId VNU, byte[] block, long usedSpace) {
        Bson filter = Filters.eq("VNU", VNU);
        Document update = new Document("$inc", new Document("usedspace", usedSpace));
        update.append("$push", new Document("blocks", new Binary(block)));//$addToSet
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        MongoSource.getObjectCollection().updateOne(filter, update, updateOptions);
        if (MongoSource.getProxy() != null) {
            ObjectUpdateLog uplog = new ObjectUpdateLog();
            uplog.setVNU(VNU);
            uplog.setUsedspace(usedSpace);
            uplog.setBlock(block);
            LogMessage log = new LogMessage(Op_Object_Block_Update, uplog);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync blocks of data referenced by objects");
        }
    }

    public static boolean listNewObject() throws Throwable {
        long curtime = System.currentTimeMillis();
        Document sort = new Document("_id", 1);
        FindIterable<Document> fi = MongoSource.getObjectNewCollection().find().sort(sort).limit(100);
        List<Document> needDelete = new ArrayList();
        for (Document doc : fi) {
            long time = doc.getLong("time");
            if (curtime - time > ServerConfig.PPC * ServerConfig.PMS) {
                needDelete.add(doc);
            } else {
                break;
            }
        }
        for (Document doc : needDelete) {
            int userid = doc.getInteger("userid");
            long cost = doc.getLong("costPerCycle");
            EOSClient.setUserFee(cost, doc.getString("username"), userid);
            UserAccessor.updateUser(userid, cost);
            Bson filter = Filters.eq("_id", doc.getObjectId("_id"));
            MongoSource.getObjectNewCollection().deleteOne(filter);
        }
        return needDelete.size() > 0;
    }

    public static void addNewObject(ObjectId id, long costPerCycle, int userid, String username) {
        Document update = new Document("_id", id);
        update.append("costPerCycle", costPerCycle);
        update.append("userid", userid);
        update.append("time", System.currentTimeMillis());
        update.append("username", username);
        MongoSource.getObjectNewCollection().insertOne(update);
    }

    public static boolean isObjectExists(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document fields = new Document("NLINK", 1);
        fields.append("VNU", 1);
        fields.append("blocks", 1);
        Document doc = MongoSource.getObjectCollection().find(filter).projection(fields).first();
        if (doc == null) {
            return false;
        } else {
            meta.fill(doc);
            return true;
        }
    }

    public static ObjectMeta getObject(ObjectId VNU) {
        Bson filter = Filters.eq("VNU", VNU);
        Document doc = MongoSource.getObjectCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(doc);
        }
    }

    public static ObjectMeta getObject(int uid, byte[] VHW) {
        ObjectMeta meta = new ObjectMeta(uid, VHW);
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document doc = MongoSource.getObjectCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(doc);
        }
    }
}
