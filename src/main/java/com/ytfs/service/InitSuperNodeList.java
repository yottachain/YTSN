package com.ytfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.ytfs.service.dao.MongoSource;
import io.jafka.jeos.util.KeyUtil;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.Document;

public class InitSuperNodeList {

    private static final Logger LOG = Logger.getLogger(InitSuperNodeList.class);

    private static final String DATABASENAME = "yotta1";
    private static final String TABLE_NAME = "SuperNode";
    private static final String INDEX_NAME = "pubkey";
    private static MongoDatabase database;
    private static MongoCollection<Document> sn_collection;

    public static void main(String[] args) throws Exception {
        MongoClient client = MongoSource.getMongoClient();
        database = client.getDatabase(DATABASENAME);
        init_collection();
        String path = System.getProperty("snlist.conf", "conf/snlist.properties");
        ObjectMapper mapper = new ObjectMapper();
        List ls = mapper.readValue(new File(path), List.class);
        try {
            for (Object obj : ls) {
                Map map = (Map) obj;
                writeNode(map);
            }
            LOG.info("OK! Insert count:" + ls.size());
        } catch (Exception r) {
            LOG.error("ERR! " + r.getMessage());
        }
    }

    private static void writeNode(Map map) throws Exception {
        Document update = new Document("_id", (int) map.get("Number"));
        update.append("nodeid", map.get("ID").toString());
        String privatekey = map.get("PrivateKey").toString();
        String publickey = KeyUtil.toPublicKey(privatekey);
        update.append("privkey", privatekey);
        update.append("pubkey", publickey);
        List addrs = (List) map.get("Addrs");
        update.append("addrs", addrs);
        try {
            sn_collection.insertOne(update);
            LOG.info("Insert OK:" + update);
        } catch (Exception r) {
            if (r.getMessage().contains("duplicate key")) {
                throw new Exception("Repeated execution initialization.");
            } else {
                throw r;
            }
        }
    }

    private static void init_collection() {
        sn_collection = database.getCollection(TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = sn_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(INDEX_NAME);
            sn_collection.createIndex(Indexes.ascending(INDEX_NAME), indexOptions);
        }
        LOG.info("Create table " + TABLE_NAME);
    }
}
