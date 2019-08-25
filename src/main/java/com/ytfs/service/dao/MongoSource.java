package com.ytfs.service.dao;

import com.ytfs.service.dao.sync.Proxy;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import org.apache.log4j.Logger;
import org.bson.Document;

public class MongoSource {

    private static final String DATABASENAME = "metabase";

    //用户表
    public static final String USER_TABLE_NAME = "users";
    private static final String USER_INDEX_NAME = "KUEp";
    //用户文件去重表
    public static final String OBJECT_TABLE_NAME = "objects";
    private static final String OBJECT_INDEX_NAME = "VNU";
    public static final String OBJECT_NEW_TABLE_NAME = "objects_new";

    //数据块源信息表
    public static final String BLOCK_TABLE_NAME = "blocks";
    private static final String BLOCK_INDEX_VHP_VHB = "VHP_VHB";//唯一
    public static final String BLOCK_DAT_TABLE_NAME = "blocks_data";
    //分片元数据
    public static final String SHARD_TABLE_NAME = "shards";

    //bucket
    public static final String BUCKET_TABLE_NAME = "buckets";
    private static final String BUCKET_INDEX_NAME = "UID_NAME";//唯一

    //file
    public static final String FILE_TABLE_NAME = "files";
    private static final String FILE_INDEX_NAME = "BID_NAME";//唯一

    private static MongoSource source = null;

    /**
     * @return the serverAddress
     */
    public static List<ServerAddress> getServerAddress() {
        newInstance();
        return source.serverAddress;
    }

    /**
     * @return the serverAddress
     */
    public static String getAuth() {
        newInstance();
        return source.authString;
    }

    public static MongoSource getMongoSource() {
        newInstance();
        return source;
    }

    private static void newInstance() {
        if (source != null) {
            return;
        }
        try {
            synchronized (MongoSource.class) {
                if (source == null) {
                    source = new MongoSource();
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

    static MongoCollection<Document> getUserCollection() {
        newInstance();
        return source.user_collection;
    }

    static MongoCollection<Document> getObjectCollection() {
        newInstance();
        return source.object_collection;
    }

    static MongoCollection<Document> getObjectNewCollection() {
        newInstance();
        return source.object_new_collection;
    }

    static MongoCollection<Document> getBlockCollection() {
        newInstance();
        return source.block_collection;
    }

    static MongoCollection<Document> getBlockDatCollection() {
        newInstance();
        return source.block_dat_collection;
    }

    static MongoCollection<Document> getShardCollection() {
        newInstance();
        return source.shard_collection;
    }

    static MongoCollection<Document> getBucketCollection() {
        newInstance();
        return source.bucket_collection;
    }

    static MongoCollection<Document> getFileCollection() {
        newInstance();
        return source.file_collection;
    }

    static Proxy getProxy() {
        newInstance();
        return source.proxy;
    }

    public static MongoClient getMongoClient() {
        newInstance();
        return source.client;
    }

    public static void terminate() {
        synchronized (MongoSource.class) {
            if (source != null) {
                source.client.close();
                source = null;
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(MongoSource.class);
    private Proxy proxy = null;
    private MongoClient client = null;
    private MongoDatabase database;
    private MongoCollection<Document> user_collection = null;
    private MongoCollection<Document> object_collection = null;
    private MongoCollection<Document> object_new_collection = null;
    private MongoCollection<Document> block_collection = null;
    private MongoCollection<Document> block_dat_collection = null;
    private MongoCollection<Document> shard_collection = null;
    private MongoCollection<Document> bucket_collection = null;
    private MongoCollection<Document> file_collection = null;

    private List<ServerAddress> serverAddress;
    private String authString = "";

    private MongoSource() throws MongoException {
        String path = System.getProperty("mongo.conf", "conf/mongo.properties");
        try (InputStream inStream = new FileInputStream(path)) {
            Properties p = new Properties();
            p.load(inStream);
            init(p);
            init_user_collection();
            init_object_collection();
            init_block_collection();
            init_bucket_collection();
            init_file_collection();
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            throw e instanceof MongoException ? (MongoException) e : new MongoException(e.getMessage());
        }
    }

    private ServerAddress toAddress(String host) {
        if (host.trim().isEmpty()) {
            return null;
        }
        String[] addr = host.trim().split(":");
        try {
            return new ServerAddress(addr[0], Integer.parseInt(addr[1]));
        } catch (NumberFormatException d) {
            LOG.warn("Invalid server address:[" + host + "]");
            return null;
        }
    }

    private void init(Properties p) {
        String hostlist = p.getProperty("serverlist");
        if (hostlist == null || hostlist.trim().isEmpty()) {
            throw new MongoException("No serverlist is specified in the MongoSource.properties file.");
        }
        String proxyuri = p.getProperty("proxy");
        if (!(proxyuri == null || proxyuri.trim().isEmpty())) {
            proxy = new Proxy(proxyuri.trim());
        }
        String[] hosts = hostlist.trim().split(",");
        List<ServerAddress> addrs = new ArrayList<>();
        for (String host : hosts) {
            ServerAddress addr = toAddress(host);
            if (addr != null) {
                addrs.add(addr);
                LOG.info("[" + addr.toString() + "]Add to the server list...");
            }
        }
        serverAddress = addrs;
        if (addrs.isEmpty()) {
            throw new MongoException("No serverlist is specified in the MongoSource.properties file.");
        }
        MongoCredential credential = null;
        String username = p.getProperty("username", "").trim();
        if (!username.isEmpty()) {
            String password = p.getProperty("password", "").trim();
            credential = MongoCredential.createScramSha1Credential(username, "admin", password.toCharArray());
            authString = username + ":" + password+"@";
        }
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        if (credential != null) {
            builder = builder.credential(credential);
        }
        String ssl = p.getProperty("ssl", "false").trim();
        if (ssl.equalsIgnoreCase("true")) {
            builder = builder.applyToSslSettings(build -> build.enabled(true));
        }
        String compressors = p.getProperty("compressors", "").trim();
        if (!compressors.isEmpty()) {
            List<MongoCompressor> comps = new ArrayList<>();
            if (compressors.toLowerCase().contains("zlib")) {
                comps.add(MongoCompressor.createZlibCompressor());
            }
            if (compressors.toLowerCase().contains("snappy")) {
                comps.add(MongoCompressor.createSnappyCompressor());
            }
            if (!comps.isEmpty()) {
                builder = builder.compressorList(comps);
            }
        }
        MongoClientSettings settings = builder.applyToClusterSettings(build -> build.hosts(addrs)).build();
        client = MongoClients.create(settings);
        LOG.info("Successful connection to Mongo server.");
        database = client.getDatabase(DATABASENAME);
    }

    private void init_user_collection() {
        user_collection = database.getCollection(USER_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = user_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(USER_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(USER_INDEX_NAME);
            user_collection.createIndex(Indexes.ascending("KUEp"), indexOptions);
        }
        LOG.info("Successful creation of user tables.");
    }

    private void init_object_collection() {
        object_collection = database.getCollection(OBJECT_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = object_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(OBJECT_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(OBJECT_INDEX_NAME);
            object_collection.createIndex(Indexes.ascending("VNU"), indexOptions);
        }
        object_new_collection = database.getCollection(OBJECT_NEW_TABLE_NAME);
        LOG.info("Successful creation of object tables.");
    }

    private void init_block_collection() {
        block_collection = database.getCollection(BLOCK_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = block_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(BLOCK_INDEX_VHP_VHB)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(BLOCK_INDEX_VHP_VHB);
            block_collection.createIndex(Indexes.ascending("VHP", "VHB"), indexOptions);
        }
        block_dat_collection = database.getCollection(BLOCK_DAT_TABLE_NAME);
        shard_collection = database.getCollection(SHARD_TABLE_NAME);
        LOG.info("Successful creation of data block tables.");
    }

    private void init_bucket_collection() {
        bucket_collection = database.getCollection(BUCKET_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = bucket_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(BUCKET_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(BUCKET_INDEX_NAME);
            bucket_collection.createIndex(Indexes.ascending("userId", "bucketName"), indexOptions);
        }
        LOG.info("Successful creation of user bucket table.");
    }

    private void init_file_collection() {
        file_collection = database.getCollection(FILE_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = file_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(FILE_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(FILE_INDEX_NAME);
            file_collection.createIndex(Indexes.ascending("bucketId", "fileName"), indexOptions);
        }
        LOG.info("Successful creation of user file table.");
    }
}
