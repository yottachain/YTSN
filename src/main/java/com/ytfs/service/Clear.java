package com.ytfs.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.ytfs.service.dao.MongoSource;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

public class Clear {

    public static void main(String[] args) throws Exception {
        MongoClient client = MongoSource.getMongoClient();
        client.getDatabase("cache").drop();
        client.getDatabase("metabase").drop();
        MongoIterable<String> dbs = client.listDatabaseNames();
        List<String> ls = new ArrayList();
        for (String ss : dbs) {
            if (ss.startsWith("usermeta_")) {
                ls.add(ss);
            }
        }
        for (String db : ls) {
            client.getDatabase(db).drop();
        }
        MongoDatabase database = client.getDatabase("yotta");
        database.getCollection("ErrorNode").deleteMany(new Document());
        database.getCollection("Node").deleteMany(new Document());
        database.getCollection("Shards").deleteMany(new Document());
        database.getCollection("SpotCheck").deleteMany(new Document());
    }
}
