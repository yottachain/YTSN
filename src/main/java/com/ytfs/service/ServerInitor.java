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
import static com.ytfs.service.InitSuperNodeList.sha256;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        } catch (Exception d) {
            throw new IOException("The 'space_factor' parameter is not configured.");
        }
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
                    SecretKeySpec skeySpec = new SecretKeySpec(sha256(), "AES");
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
