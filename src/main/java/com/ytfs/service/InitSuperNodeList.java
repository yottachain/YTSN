package com.ytfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.superNodeID;
import com.ytfs.service.dao.MongoSource;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.p2phost.utils.Base58;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;
import org.bson.Document;

public class InitSuperNodeList {

    private static final Logger LOG = Logger.getLogger(InitSuperNodeList.class);

    private static String DATABASENAME;
    private static final String TABLE_NAME = "SuperNode";
    private static final String INDEX_NAME = "pubkey";
    private static MongoDatabase database;
    private static MongoCollection<Document> sn_collection;

    public static void main(String[] args) throws Exception {
        try {
            update();
            LOG.info("Update conf!");
        } catch (Exception r) {
            LOG.error("Update conf ERR! " + r.getMessage());
        }
        String s = System.getenv("IPFS_DBNAME_SNID");
        boolean IPFS_DBNAME_SNID = s != null && s.trim().equalsIgnoreCase("yes");
        if (IPFS_DBNAME_SNID) {
            DATABASENAME = "yotta" + ServerConfig.superNodeID;
        } else {
            DATABASENAME = "yotta";
        }
        MongoClient client = MongoSource.getMongoClient();
        database = client.getDatabase(DATABASENAME);
        init_collection();
        String path = System.getProperty("snlist.conf", "../conf/snlist.properties");
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

    private static void update() throws Exception {
        String path = System.getProperty("server.conf", "../conf/server.properties");
        InputStream is = new FileInputStream(path);
        Properties p = new Properties();
        try {
            if (is == null) {
                throw new IOException("No properties file could be found for ytfs service");
            }
            p.load(is);
        } finally {
            is.close();
        }
        try {
            String ss = p.getProperty("superNodeID");
            superNodeID = Integer.parseInt(ss);
            if (superNodeID < 0 || superNodeID > 31) {
                throw new IOException();
            }
        } catch (Exception d) {
            throw new IOException("The 'superNodeID' parameter is not configured.");
        }
        String shadowPriKey = p.getProperty("ShadowPriKey");
        if (shadowPriKey != null) {
            if (!shadowPriKey.trim().isEmpty()) {
                shadowPriKey = shadowPriKey.trim();
                if (shadowPriKey.startsWith("yotta:")) {
                    return;
                }
                SecretKeySpec skeySpec = new SecretKeySpec(sha256(), "AES");
                Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
                byte[] bs = cipher.doFinal(shadowPriKey.getBytes());
                p.setProperty("ShadowPriKey", "yotta:" + Base58.encode(bs));
            }
        }
        OutputStream out = new FileOutputStream(path);
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
            bw.newLine();
            for (Enumeration<?> e = p.keys(); e.hasMoreElements();) {
                String key = (String) e.nextElement();
                String val = p.getProperty(key);
                bw.write(key + "=" + val);
                bw.newLine();
            }
            bw.flush();
        } finally {
            out.close();
        }
    }

    public static byte[] sha256() throws NoSuchAlgorithmException, IOException {
        InputStream is = InitSuperNodeList.class.getResourceAsStream("/InitSuperNodeList.class");
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] bs = new byte[1024];
            int len = 0;
            while ((len = is.read(bs)) != -1) {
                sha256.update(bs, 0, len);
            }
            return sha256.digest();
        } finally {
            is.close();
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
