package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_Block_Update;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_NLINK_INC;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_Object_New;
import com.ytfs.service.dao.sync.ObjectMetaLog;
import com.ytfs.service.dao.sync.ObjectUpdateLog;
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
        Document doc = MongoSource.getObjectCollection(meta.getUserID()).find(filter).projection(fields).first();
        if (doc == null) {
            MongoSource.getObjectCollection(meta.getUserID()).insertOne(meta.toDocument());
        } else {
            if (doc.getLong("length") != meta.getLength()) {
                Bson filter1 = Filters.eq("VNU", meta.getVNU());
                Document data = new Document("length", meta.getLength());
                Document update = new Document("$set", data);
                MongoSource.getObjectCollection(meta.getUserID()).updateOne(filter1, update);
            }
        }
        if (MongoSource.getProxy() != null) {
            ObjectMetaLog metalog = new ObjectMetaLog(meta);
            LogMessage log = new LogMessage(Op_Object_New, metalog);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync object " + metalog.getVNU());
        }
    }

    public static void decObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getVHW()));
        Document update = new Document("$inc", new Document("NLINK", -1));
        MongoSource.getObjectCollection(meta.getUserID()).updateOne(filter, update);
        if (MongoSource.getProxy() != null) {
            LogMessage log = new LogMessage(Op_Object_NLINK_INC, meta.getVHW());
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync object NLINK");
        }
    }

    public static void incObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getVHW()));
        Document update = new Document("$inc", new Document("NLINK", 1));
        MongoSource.getObjectCollection(meta.getUserID()).updateOne(filter, update);
        if (MongoSource.getProxy() != null) {
            LogMessage log = new LogMessage(Op_Object_NLINK_INC, meta.getVHW());
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync object NLINK");
        }
    }

    public static void getObjectAndUpdateNLINK(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getVHW()));
        Document data = new Document("NLINK", 1);
        Document update = new Document("$set", data);
        Document doc = MongoSource.getObjectCollection(meta.getUserID()).findOneAndUpdate(filter, update);
        if (doc != null) {
            meta.fill(doc);
            if (MongoSource.getProxy() != null) {
                LogMessage log = new LogMessage(Op_Object_NLINK_INC, meta.getVHW());
                MongoSource.getProxy().post(log);
                LOG.debug("DBlog: sync object NLINK");
            }
        }
    }

    public static void updateObject(int userid, ObjectId VNU, byte[] block, long usedSpace) {
        Bson filter = Filters.eq("VNU", VNU);
        Document update = new Document("$inc", new Document("usedspace", usedSpace));
        update.append("$push", new Document("blocks", new Binary(block)));//$addToSet
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        MongoSource.getObjectCollection(userid).updateOne(filter, update, updateOptions);
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

    public static boolean isObjectExists(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getVHW()));
        Document fields = new Document("NLINK", 1);
        fields.append("VNU", 1);
        fields.append("blocks", 1);
        Document doc = MongoSource.getObjectCollection(meta.getUserID()).find(filter).projection(fields).first();
        if (doc == null) {
            return false;
        } else {
            meta.fill(doc);
            return true;
        }
    }

    public static ObjectMeta getObject(int userid, ObjectId VNU) {
        Bson filter = Filters.eq("VNU", VNU);
        Document doc = MongoSource.getObjectCollection(userid).find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(userid, doc);
        }
    }

    public static ObjectMeta getObject(int uid, byte[] VHW) {
        Bson filter = Filters.eq("_id", new Binary(VHW));
        Document doc = MongoSource.getObjectCollection(uid).find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(uid, doc);
        }
    }
}
