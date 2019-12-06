package com.ytfs.service;

import com.mongodb.ServerAddress;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.LogConfigurator;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.lrc.MemoryCache;
import com.ytfs.common.codec.lrc.ShardLRCEncoder;
import static com.ytfs.common.conf.ServerConfig.*;
import com.ytfs.common.eos.BpList;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.http.HttpServerBoot;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.NodeManager;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.servlet.FromBPMsgDispatcher;
import com.ytfs.service.servlet.FromNodeMsgDispatcher;
import com.ytfs.service.servlet.FromUserMsgDispatcher;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.p2phost.interfaces.BPNodeCallback;
import io.yottachain.p2phost.interfaces.NodeCallback;
import io.yottachain.p2phost.interfaces.UserCallback;
import io.yottachain.p2phost.utils.Base58;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServerInitor {

    private static final Logger LOG = Logger.getLogger(ServerInitor.class);

    public static byte[] makeBytes(int length) {
        Random ran = new Random();
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int ii = 0; ii < length / 8; ii++) {
            long l = ran.nextLong();
            buf.putLong(l);
        }
        return buf.array();
    }

    private static void testLRC() throws IOException {
        MemoryCache.init();
        BlockEncrypted b = new BlockEncrypted();
        b.setData(makeBytes(1024 * 1024 * 2 - 1024 * 16));
        ShardLRCEncoder encoder = new ShardLRCEncoder(b);
        encoder.encode();

    }

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
        try {
            testLRC();
        } catch (IOException ex) {
            LOG.error("Init err.", ex);
        }
        for (int ii = 0; ii < 1000; ii++) {
            try {
                List<ServerAddress> addrs = MongoSource.getServerAddress();
                NodeManager.start(addrs, MongoSource.getAuth(), eosURI, BPAccount, ShadowAccount, ShadowPriKey, contractAccount, contractOwnerD, superNodeID);
                privateKey = YottaNodeMgmt.getSuperNodePrivateKey(superNodeID);
                SNDSP = Base58.decode(privateKey);
                SuperNodeList.isServer = true;
                Sequence.initUserID_seq();
                BpList.init(SuperNodeList.getSuperNodeList());
                break;
            } catch (Throwable r) {
                LOG.error("Mongo client initialization failed:" + r.getMessage());
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
                UserCallback userCallback = new FromUserMsgDispatcher();
                BPNodeCallback bPNodeCallback = new FromBPMsgDispatcher();
                NodeCallback nodeCallback = new FromNodeMsgDispatcher();
                P2PUtils.register(userCallback, bPNodeCallback, nodeCallback);
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
        httpBindip = p.getProperty("httpBindip", "").trim();
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
