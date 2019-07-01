package com.ytfs.service;

import com.mongodb.ServerAddress;
import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.eosURI;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.LogConfigurator;
import static com.ytfs.common.conf.ServerConfig.*;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.http.HttpServerBoot;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.NodeManager;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.FromBPMsgDispatcher;
import com.ytfs.service.servlet.FromNodeMsgDispatcher;
import com.ytfs.service.servlet.FromUserMsgDispatcher;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.interfaces.BPNodeCallback;
import io.yottachain.p2phost.interfaces.NodeCallback;
import io.yottachain.p2phost.interfaces.UserCallback;
import io.yottachain.p2phost.utils.Base58;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
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
        try {
            String level = WrapperManager.getProperties().getProperty("wrapper.log4j.loglevel", "INFO");
            String path = WrapperManager.getProperties().getProperty("wrapper.log4j.logfile");
            LogConfigurator.configPath(path == null ? null : new File(path), level);
            load();
            List<ServerAddress> addrs = MongoSource.getServerAddress();
            NodeManager.start(addrs, eosURI, BPAccount, BPPriKey, contractAccount, contractOwnerD, superNodeID);
            privateKey = YottaNodeMgmt.getSuperNodePrivateKey(superNodeID);
            SNDSP = Base58.decode(privateKey);
            SuperNodeList.isServer = true;
            int sncount = SuperNodeList.getSuperNodeCount();
            MongoSource.getMongoSource().init_seq_collection(sncount);
        } catch (NodeMgmtException | IOException e) {
            LOG.error("Init err.", e);
            System.exit(0);
        }
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = ServerConfig.port + ii;
                P2PUtils.start(port, ServerConfig.privateKey);
                UserCallback userCallback = new FromUserMsgDispatcher();
                BPNodeCallback bPNodeCallback = new FromBPMsgDispatcher();
                NodeCallback nodeCallback = new FromNodeMsgDispatcher();
                P2PUtils.register(userCallback, bPNodeCallback, nodeCallback);
                LOG.info("P2P initialization completed, port " + port);
                break;
            } catch (Exception r) {
                LOG.error("P2P initialization failed!", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
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
        }
        BPAccount = p.getProperty("BPAccount");
        if (BPAccount == null || BPAccount.trim().isEmpty()) {
            throw new IOException("The 'BPAccount' parameter is not configured.");
        }
        BPPriKey = p.getProperty("BPPriKey");
        if (BPPriKey == null || BPPriKey.trim().isEmpty()) {
            throw new IOException("The 'BPPriKey' parameter is not configured.");
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
