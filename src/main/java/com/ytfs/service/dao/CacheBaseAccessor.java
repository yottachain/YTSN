package com.ytfs.service.dao;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
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

    public static void addDNI(int nid, byte[] vhf, boolean delete) {
        Document update = new Document();
        update.append("nodeId", nid);
        update.append("vhf", new Binary(vhf));
        update.append("delete", delete);
        CacheBaseSource.getDNICollection().insertOne(update);
    }

    public static void addDNI(List<Document> doc) {
        CacheBaseSource.getDNICollection().insertMany(doc);
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

    public static boolean listNewObject() throws Throwable {
        /*
        long curtime = System.currentTimeMillis();
        FindIterable<Document> fi = CacheBaseSource.getObjectNewCollection().find().limit(1000);
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
            CacheBaseSource.getObjectNewCollection().deleteOne(filter);
        }*/
        return false;
    }

}
