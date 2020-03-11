package com.ytfs.service.dao;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.ytfs.common.conf.ServerConfig;
import org.bson.Document;

public class DNIMetaSource {

    private static final String DATABASENAME;

    static {
        String s = System.getenv("IPFS_DBNAME_SNID");
        boolean IPFS_DBNAME_SNID = s != null && s.trim().equalsIgnoreCase("yes");
        if (IPFS_DBNAME_SNID) {
            DATABASENAME = "yotta" + "_" + ServerConfig.superNodeID;
        } else {
            DATABASENAME = "yotta";
        }
    }

    public static final String DNI_TABLE_NAME = "Shards";
    public static final String SPACESUN_TABLE_NAME = "SpaceSum";

    private final MongoDatabase database;
    private MongoCollection<Document> dni_collection = null;
    private MongoCollection<Document> space_sum_collection = null;
    private static final String INDEX_SNID_RELATIONSHIP = "SNID_RELATIONSHIP";   //唯一

    public DNIMetaSource(MongoClient client) {
        this.database = client.getDatabase(DATABASENAME);
        this.dni_collection = this.database.getCollection(DNI_TABLE_NAME);
        this.space_sum_collection = this.database.getCollection(SPACESUN_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = space_sum_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(INDEX_SNID_RELATIONSHIP)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(INDEX_SNID_RELATIONSHIP);
            space_sum_collection.createIndex(Indexes.ascending("snid", "mowner"), indexOptions);
        }
    }

    /**
     * @return the bucket_collection
     */
    public MongoCollection<Document> getDNI_collection() {
        return dni_collection;
    }

    /**
     * @return the bucket_collection
     */
    public MongoCollection<Document> getSpaceSum_collection() {
        return space_sum_collection;
    }
}
