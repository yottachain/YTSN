package com.ytfs.service.dao;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ytfs.common.conf.ServerConfig;
import org.bson.Document;

public class CacheBaseSource {

    private static final String DATABASENAME;

    static {
        String s = System.getenv("IPFS_DBNAME_SNID");
        boolean IPFS_DBNAME_SNID = s != null && s.trim().equalsIgnoreCase("yes");
        if (IPFS_DBNAME_SNID) {
            DATABASENAME = "cache" + "_" + ServerConfig.superNodeID;
        } else {
            DATABASENAME = "cache";
        }
    }

    public static final String DNI_CACHE_NAME = "dnis";
    public static final String OBJECT_NEW_TABLE_NAME = "objects_new";

    private static CacheBaseSource source = null;

    private static void newInstance() {
        if (source != null) {
            return;
        }
        try {
            synchronized (CacheBaseSource.class) {
                if (source == null) {
                    source = new CacheBaseSource(MongoSource.getMongoClient());
                }
            }
        } catch (Exception r) {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException ex) {
            }
            throw new MongoException(r.getMessage());
        }
    }

    static MongoCollection<Document> getDNICollection() {
        newInstance();
        return source.dni_collection;
    }

    static MongoCollection<Document> getObjectNewCollection() {
        newInstance();
        return source.object_new_collection;
    }

    private MongoCollection<Document> dni_collection = null;
    private MongoCollection<Document> object_new_collection = null;

    private CacheBaseSource(MongoClient client) {
        MongoDatabase database = client.getDatabase(DATABASENAME);
        this.dni_collection = database.getCollection(DNI_CACHE_NAME);
        this.object_new_collection = database.getCollection(OBJECT_NEW_TABLE_NAME);
    }

}
