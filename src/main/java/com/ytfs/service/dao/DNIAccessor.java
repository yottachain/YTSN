package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.service.packet.node.ListDNIResp;
import java.util.Arrays;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class DNIAccessor {

    public static boolean findDNI(int nodeId, byte[] vhf) {
        Bson filter = Filters.eq("minerID", nodeId);
        FindIterable<Document> it = MongoSource.getDNIMetaSource().getDNI_collection().find(filter);
        for (Document doc : it) {
            byte[] bs = ((Binary) doc.get("shard")).getData();
            byte[] VHF = new byte[16];
            System.arraycopy(bs, bs.length - 16, VHF, 0, 16);
            if (Arrays.equals(vhf, VHF)) {
                return true;
            }
        }
        return false;
    }

    public static ListDNIResp listDNI(int nodeId, String nextId, int limit) {
        if (limit < 10) {
            limit = 10;
        }
        if (limit > 5000) {
            limit = 5000;
        }
        ObjectId id = null;
        if (nextId != null) {
            id = new ObjectId(nextId);
        }
        Bson filter;
        if (id != null) {
            Bson bson1 = Filters.gt("_id", id);
            Bson bson2 = Filters.eq("minerID", nodeId);
            filter = Filters.and(bson1, bson2);
        } else {
            filter = Filters.eq("minerID", nodeId);
        }
        Document fields = new Document("_id", 1);
        fields.append("shard", 1);
        Bson sort = new Document("_id", 1);
        FindIterable<Document> it = MongoSource.getDNIMetaSource().getDNI_collection().find(filter).projection(fields).sort(sort).limit(limit);
        ListDNIResp resp = new ListDNIResp();
        ObjectId lastObjectId = null;
        for (Document doc : it) {
            if (System.currentTimeMillis() - doc.getObjectId("_id").getTimestamp() * 1000L < 1000L * 60L * 5L) {
                break;
            }
            lastObjectId = doc.getObjectId("_id");
            byte[] bs = ((Binary) doc.get("shard")).getData();
            byte[] VHF = new byte[16];
            System.arraycopy(bs, bs.length - 16, VHF, 0, 16);
            resp.addVHF(VHF);
        }
        if (lastObjectId != null) {
            resp.setNextId(lastObjectId.toHexString());
        } else {
            resp.setNextId(nextId);
        }
        return resp;
    }

}
