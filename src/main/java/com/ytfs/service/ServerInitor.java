package com.ytfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ServerAddress;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.LogConfigurator;
import static com.ytfs.common.conf.ServerConfig.*;
import com.ytfs.common.eos.BpList;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.http.HttpServerBoot;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.NodeManager;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.servlet.MsgDispatcher;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.p2phost.interfaces.Callback;
import io.yottachain.p2phost.utils.Base58;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServerInitor {

    private static final Logger LOG = Logger.getLogger(ServerInitor.class);

    public static void stop() {
        P2PUtils.stop();
        HttpServerBoot.stopHttpServer();
        MongoSource.terminate();
        GlobleThreadPool.shutdown();
    }

    public static void init() {
        System.out.println("SN is starting......");
        try {
            String level = WrapperManager.getProperties().getProperty("wrapper.log4j.loglevel", "INFO");
            String path = WrapperManager.getProperties().getProperty("wrapper.log4j.logfile");
            LogConfigurator.configPath(path == null ? null : new File(path), level);
            load();
        } catch (IOException e) {
            LOG.error("Init err.", e);
            System.exit(0);
        }
        for (int ii = 0; ii < 1000; ii++) {
            try {
                List<ServerAddress> addrs = MongoSource.getServerAddress();
                NodeManager.start(addrs, MongoSource.getAuth(), eosURI, BPAccount, ShadowAccount, ShadowPriKey, contractAccount, contractOwnerD, superNodeID);
                privateKey = YottaNodeMgmt.getSuperNodePrivateKey(superNodeID);
                SNDSP = Base58.decode(privateKey);
                SuperNodeList.isServer = true;
                YottaNodeMgmt.setMaster(SuperNodeList.isMaster());
                Sequence.initUserID_seq();
                BpList.init(loadbplist());
                break;
            } catch (Throwable r) {
                LOG.error("Mongo client initialization failed:", r);
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ex) {
                    System.exit(0);
                }
                MongoSource.terminate();
            }
        }
        for (int ii = 0; ii < 100; ii++) {
            try {
                int port = ServerConfig.port + ii;
                P2PUtils.start(port, ServerConfig.privateKey);
                Callback nodeCallback = new MsgDispatcher();
                P2PUtils.register(nodeCallback);
                break;
            } catch (Exception r) {
                LOG.error("P2P initialization failed!", r);
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ex) {
                    System.exit(0);
                }
                P2PUtils.stop();
            }
        }
        try {
            HttpServerBoot.startHttpServer();
        } catch (Exception r) {
            LOG.error("Http server failed to start!", r);
        }
    }

    private static List<String> loadbplist() {
        String path = System.getProperty("bplist.conf", "../conf/bplist.properties");
        try {
            InputStream is = new FileInputStream(path);
            if (is == null) {
                LOG.error("No properties file 'bplist.conf' could be found for ytfs service");
                return null;
            }
        } catch (Exception e) {
            LOG.error("No properties file 'bplist.conf' could be found for ytfs service");
            return null;
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            List ls = mapper.readValue(new File(path), List.class);
            List<String> iplist = new ArrayList();
            for (Object obj : ls) {
                iplist.add(obj.toString());
            }
            return iplist;
        } catch (Exception e) {
            LOG.error("No properties file 'bplist.conf' could be found for ytfs service");
        }
        return null;
    }

    public static byte[] sha256Digest() throws NoSuchAlgorithmException, IOException {
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

    private static void load() throws IOException {
        String path = System.getProperty("server.conf", "../conf/server.properties");
        LOG.info("Read conf:" + path);
        InputStream is = new FileInputStream(path);
        if (is == null) {
            throw new IOException("No properties file could be found for ytfs service");
        }
        Properties p = new Properties();
        p.load(is);
        try {
            String ss = p.getProperty("superNodeID");
            superNodeID = Integer.parseInt(ss);
            if (superNodeID < 0 || superNodeID > 31) {
                throw new IOException();
            }
        } catch (Exception d) {
            throw new IOException("The 'superNodeID' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("port", "9999").trim();
            port = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'port' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("httpPort", "8080").trim();
            httpPort = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'httpPort' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("space_factor", "100").trim();
            space_factor = Integer.parseInt(ss);
            if (space_factor < 0) {
                space_factor = 100;
            }
            if (space_factor > 100) {
                space_factor = 100;
            }
        } catch (Exception d) {
            throw new IOException("The 'space_factor' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("isBackup", "0").trim();
            isBackup = Integer.parseInt(ss);
            if (isBackup < 0) {
                isBackup = 0;
            }
            if (isBackup > 1) {
                isBackup = 1;
            }
        } catch (Exception d) {
            throw new IOException("The 'isBackup' parameter is not configured.");
        }
        selfIp = p.getProperty("selfIp");
        if (selfIp == null || selfIp.trim().isEmpty()) {
            selfIp = null;
            LOG.warn("The 'selfIp' parameter is not configured.");
        } else {
            selfIp = selfIp.trim();
        }
        s3Version = p.getProperty("s3Version");
        if (s3Version == null || s3Version.trim().isEmpty()) {
            s3Version = null;
        }
        try {
            String ss = p.getProperty("sendShardInterval", "1000").trim();
            sendShardInterval = Integer.parseInt(ss);
            if (sendShardInterval < 0) {
                sendShardInterval = 0;
            }
            if (sendShardInterval > 1000 * 60 * 3) {
                sendShardInterval = 1000 * 60 * 3;
            }
        } catch (Exception d) {
            throw new IOException("The 'sendShardInterval' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("lsCacheExpireTime", "30").trim();
            lsCacheExpireTime = Integer.parseInt(ss);
            if (lsCacheExpireTime < 5) {
                lsCacheExpireTime = 5;
            }
            if (lsCacheExpireTime > 60 * 5) {
                lsCacheExpireTime = 60 * 5;
            }
        } catch (Exception d) {
            lsCacheExpireTime = 30;
        }
        try {
            String ss = p.getProperty("lsCachePageNum", "10").trim();
            lsCachePageNum = Integer.parseInt(ss);
            if (lsCachePageNum < 1) {
                lsCachePageNum = 1;
            }
            if (lsCachePageNum > 100) {
                lsCachePageNum = 100;
            }
        } catch (Exception d) {
            lsCachePageNum = 10;
        }
        try {
            String ss = p.getProperty("lsIntervalLimit", "10").trim();
            lsIntervalLimit = Integer.parseInt(ss);
            if (lsIntervalLimit < 1) {
                lsIntervalLimit = 1;
            }
            if (lsIntervalLimit > 60 * 5) {
                lsIntervalLimit = 60 * 5;
            }
        } catch (Exception d) {
            lsIntervalLimit = 10;
        }
        try {
            String ss = p.getProperty("lsCacheMaxSize", "20000").trim();
            lsCacheMaxSize = Integer.parseInt(ss);
            if (lsCacheMaxSize < 10000) {
                lsCacheMaxSize = 10000;
            }
            if (lsCacheMaxSize > 200000) {
                lsCacheMaxSize = 200000;
            }
        } catch (Exception d) {
            lsCacheMaxSize = 20000;
        }

        try {
            String ss = p.getProperty("shardNumPerNode", "8").trim();
            shardNumPerNode = Integer.parseInt(ss);
            if (shardNumPerNode < 1) {
                shardNumPerNode = 1;
            }
            if (shardNumPerNode > 164) {
                shardNumPerNode = 164;
            }
        } catch (Exception d) {
            throw new IOException("The 'shardNumPerNode' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("rebuildTaskSize", "100").trim();
            rebuildTaskSize = Integer.parseInt(ss);
            if (rebuildTaskSize < 10) {
                rebuildTaskSize = 10;
            }
            if (rebuildTaskSize > 500) {
                rebuildTaskSize = 500;
            }
        } catch (Exception d) {
            throw new IOException("The 'rebuildTaskSize' parameter is not configured.");
        }
        httpServlet = p.getProperty("httpServlet", "").trim().replaceAll(" ", "");
        httpRemoteIp = p.getProperty("httpRemoteIp", "").trim().replaceAll(" ", "");
        eosURI = p.getProperty("eosURI");
        if (eosURI == null || eosURI.trim().isEmpty()) {
            throw new IOException("The 'eosURI' parameter is not configured.");
        } else {
            URL url = new URL(eosURI.trim());
            eosURI = url.toString();
        }
        BPAccount = p.getProperty("BPAccount");
        if (BPAccount == null || BPAccount.trim().isEmpty()) {
            throw new IOException("The 'BPAccount' parameter is not configured.");
        }
        ShadowAccount = p.getProperty("ShadowAccount");
        if (ShadowAccount == null || ShadowAccount.trim().isEmpty()) {
            throw new IOException("The 'ShadowAccount' parameter is not configured.");
        }
        ShadowPriKey = p.getProperty("ShadowPriKey");
        if (ShadowPriKey == null || ShadowPriKey.trim().isEmpty()) {
            throw new IOException("The 'ShadowPriKey' parameter is not configured.");
        } else {
            if (ShadowPriKey.startsWith("yotta:")) {
                ShadowPriKey = ShadowPriKey.substring(6);
                try {
                    SecretKeySpec skeySpec = new SecretKeySpec(sha256Digest(), "AES");
                    Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                    cipher.init(Cipher.DECRYPT_MODE, skeySpec);
                    byte[] bs = cipher.doFinal(Base58.decode(ShadowPriKey.trim()));
                    ShadowPriKey = new String(bs);
                } catch (Exception d) {
                    throw new IOException("The 'ShadowPriKey' parameter is not configured.");
                }
            }
        }
        contractAccount = p.getProperty("contractAccount");
        if (contractAccount == null || contractAccount.trim().isEmpty()) {
            throw new IOException("The 'contractAccount' parameter is not configured.");
        }
        contractOwnerD = p.getProperty("contractOwnerD");
        if (contractOwnerD == null || contractOwnerD.trim().isEmpty()) {
            throw new IOException("The 'contractOwnerD' parameter is not configured.");
        }
    }
}
