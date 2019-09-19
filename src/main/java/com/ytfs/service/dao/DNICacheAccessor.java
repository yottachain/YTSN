package com.ytfs.service.dao;

import io.jafka.jeos.util.Base58;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

public class DNICacheAccessor {

    private static final Logger LOG = Logger.getLogger(DNICacheAccessor.class);
    private static final String ADDTABNAME = "addcache_";

    public static void addDNI(int snid, int nid, byte[] vhf, boolean delete) {
        try {
            Document update = new Document("_id", new Object());
            update.append("nodeId", nid);
            update.append("vhf", new Binary(vhf));
            update.append("delete", delete);
            MongoSource.getCollection(ADDTABNAME + snid).insertOne(update);
        } catch (Throwable r) {
            LOG.error("WriteDNI " + nid + "-[" + Base58.encode(vhf) + "] ERR:" + r.getMessage());
        }
    }
}
