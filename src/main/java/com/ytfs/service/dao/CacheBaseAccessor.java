package com.ytfs.service.dao;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.ytfs.common.node.SuperNodeList;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class CacheBaseAccessor {

    public static long getUserSumTime(int userId) {
        Document filter = new Document("_id", userId);
        Document doc = CacheBaseSource.getUserSumCollection().find(filter).first();
        if (doc == null) {
            return 0;
        } else {
            return doc.getLong("statTime");
        }
    }

    public static void setUserSumTime(int userId) {
        Document filter = new Document("_id", userId);
        Document update = new Document("$set", new Document("statTime", System.currentTimeMillis()));
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        CacheBaseSource.getUserSumCollection().updateOne(filter, update, updateOptions);
    }

    public static void addDNI(int nid, byte[] vhf, boolean delete) {
        Document update = new Document();
        update.append("nodeId", nid);
        update.append("vhf", new Binary(vhf));
        update.append("delete", delete);
        CacheBaseSource.getDNICollection().insertOne(update);
    }

    public static void addDNI(List<Document> doc) {
        try {
            CacheBaseSource.getDNICollection().insertMany(doc);
        } catch (MongoException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
    }

    public static Map<SuperNode, List<Document>> queryDNI(int count) {
        Map<SuperNode, List<Document>> map = new HashMap();
        FindIterable<Document> fi = CacheBaseSource.getDNICollection().find().limit(count);
        for (Document doc : fi) {
            int nodeid = doc.getInteger("nodeId");
            SuperNode sn = SuperNodeList.getDNISuperNode(nodeid);
            List<Document> ls = map.get(sn);
            if (ls == null) {
                ls = new ArrayList();
                map.put(sn, ls);
            }
            ls.add(doc);
        }
        return map;
    }

    public static void deleteDNI(ObjectId[] ids) {
        Bson bson = Filters.in("_id", ids);
        CacheBaseSource.getDNICollection().deleteMany(bson);
    }

    public static void addNewObject(ObjectId id, long usedSpace, int userid, String username, int step) {
        Document update = new Document("_id", id);
        update.append("step", step);
        update.append("usedSpace", usedSpace);
        update.append("userid", userid);
        update.append("username", username);
        try {
            CacheBaseSource.getObjectNewCollection().insertOne(update);
        } catch (MongoException r) {
            if (!(r.getMessage() != null && r.getMessage().contains("duplicate key"))) {
                throw r;
            }
        }
    }

    public static Document findOneNewObject() throws Throwable {
        Document update = new Document();
        Document doc = CacheBaseSource.getObjectNewCollection().findOneAndDelete(update);
        return doc;
    }

}
