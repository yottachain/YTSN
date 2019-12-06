package com.ytfs.service.dao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class DNIMetaSource {

    private static final String DATABASENAME = "yotta";

    public static final String DNI_TABLE_NAME = "Shards";

    private final MongoDatabase database;
    private MongoCollection<Document> dni_collection = null;

    public DNIMetaSource(MongoClient client) {
        this.database = client.getDatabase(DATABASENAME);
        this.dni_collection = this.database.getCollection(DNI_TABLE_NAME);
    }

    /**
     * @return the bucket_collection
     */
    public MongoCollection<Document> getDNI_collection() {
        return dni_collection;
    }

}
