package com.ytfs.service.dao;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.apache.log4j.Logger;
import org.bson.Document;

public class UserMetaSource {

    private static final Logger LOG = Logger.getLogger(UserMetaSource.class);

    private static final String DATABASENAME = "usermeta_";
    //bucket
    public static final String BUCKET_TABLE_NAME = "buckets";
    private static final String BUCKET_INDEX_NAME = "BUKNAME";//唯一

    //file
    public static final String FILE_TABLE_NAME = "files";
    private static final String FILE_INDEX_NAME = "BID_NAME";//唯一

    //用户文件去重表
    public static final String OBJECT_TABLE_NAME = "objects";
    private static final String OBJECT_INDEX_NAME = "VNU";

    private MongoClient client = null;
    private final int userId;
    private MongoDatabase database;
    private MongoCollection<Document> bucket_collection = null;
    private MongoCollection<Document> file_collection = null;
    private MongoCollection<Document> object_collection = null;

    public UserMetaSource(MongoClient client, int userId) {
        this.client = client;
        this.userId = userId;
        init();
    }

    private void init() {
        database = client.getDatabase(DATABASENAME + userId);
        init_bucket_collection();
        init_file_collection();
        init_object_collection();
    }

    private void init_object_collection() {
        object_collection = database.getCollection(OBJECT_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = getObject_collection().listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(OBJECT_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(OBJECT_INDEX_NAME);
            getObject_collection().createIndex(Indexes.ascending("VNU"), indexOptions);
        }
        LOG.info("Successful creation of user " + userId + " object tables.");
    }

    private void init_bucket_collection() {
        bucket_collection = database.getCollection(BUCKET_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = getBucket_collection().listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(BUCKET_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(BUCKET_INDEX_NAME);
            getBucket_collection().createIndex(Indexes.ascending("bucketName"), indexOptions);
        }
        LOG.info("Successful creation of user " + userId + " bucket table.");
    }

    private void init_file_collection() {
        file_collection = database.getCollection(FILE_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = getFile_collection().listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(FILE_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(FILE_INDEX_NAME);
            getFile_collection().createIndex(Indexes.ascending("bucketId", "fileName"), indexOptions);
        }
        LOG.info("Successful creation of user " + userId + " file table.");
    }

    /**
     * @return the bucket_collection
     */
    public MongoCollection<Document> getBucket_collection() {
        return bucket_collection;
    }

    /**
     * @return the file_collection
     */
    public MongoCollection<Document> getFile_collection() {
        return file_collection;
    }

    /**
     * @return the object_collection
     */
    public MongoCollection<Document> getObject_collection() {
        return object_collection;
    }
}
