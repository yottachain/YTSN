package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.service.ServerConfig;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class ObjectAccessor {

    public static boolean listNewObject() {
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
            UserAccessor.updateUser(doc.getInteger("userid"), doc.getLong("costPerCycle"));
            Bson filter = Filters.eq("_id", doc.getObjectId("_id"));
            MongoSource.getObjectNewCollection().deleteOne(filter);
        }
        return needDelete.size() >= 100;
    }

    public static void addNewObject(ObjectId id, long costPerCycle, int userid) {
        Document update = new Document("_id", id);
        update.append("costPerCycle", costPerCycle);
        update.append("userid", userid);
        update.append("time", System.currentTimeMillis());
        MongoSource.getObjectNewCollection().insertOne(update);
    }

    public static void incObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("$inc", new Document("NLINK", 1));
        MongoSource.getObjectCollection().updateOne(filter, update);
    }

    public static void getObjectAndUpdateNLINK(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document data = new Document("NLINK", 1);
        Document update = new Document("$set", data);
        Document doc = MongoSource.getObjectCollection().findOneAndUpdate(filter, update);
        if (doc != null) {
            meta.setNLINK(doc.getInteger("NLINK"));
            meta.setVNU(doc.getObjectId("VNU"));
            meta.setBlocks(((Binary) doc.get("blocks")).getData());
            meta.setLength(doc.getLong("length"));
            meta.setUsedspace(doc.getLong("usedspace"));
        }
    }

    public static void decObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("$inc", new Document("NLINK", -1));
        MongoSource.getObjectCollection().updateOne(filter, update);
    }

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
            meta.setNLINK(doc.getInteger("NLINK"));
            meta.setVNU(doc.getObjectId("VNU"));
            meta.setBlocks(((Binary) doc.get("blocks")).getData());
            return true;
        }
    }

    public static void updateObject(ObjectId VNU, byte[] blocks, long usedSpace) {
        Bson filter = Filters.eq("VNU", VNU);
        Document data = new Document("blocks", new Binary(blocks));
        Document update = new Document("$set", data);
        update.append("$inc", new Document("usedspace", usedSpace));
        MongoSource.getObjectCollection().updateOne(filter, update);
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
