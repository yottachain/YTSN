package com.ytfs.service;

import com.ytfs.service.utils.GlobleThreadPool;
import com.ytfs.service.utils.LogConfigurator;
import static com.ytfs.service.ServerConfig.privateKey;
import static com.ytfs.service.ServerConfig.superNodeID;
import static com.ytfs.service.ServerConfig.port;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.dao.RedisSource;
import com.ytfs.service.net.P2PUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ServerInitor {

    private static final Logger LOG = Logger.getLogger(ServerInitor.class);

    public static void stop() {
        P2PUtils.stop();
        MongoSource.terminate();
        RedisSource.terminate();
        GlobleThreadPool.shutdown();
    }

    public static void init() {
        try {
            String path = System.getProperty("logger.path", "log");
            File dir = new File(path);
            dir.mkdirs();
            LogConfigurator.configPath(new File(dir, "log"), "INFO");
            load();
        } catch (IOException e) {
            LOG.error("Init err.", e);
            System.exit(0);//循环初始化
        }
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = ServerConfig.port + ii;
                P2PUtils.start(port, ServerConfig.privateKey);
                P2PUtils.register();
                LOG.info("P2P initialization completed, port " + port);
                break;
            } catch (Exception r) {
                LOG.info("P2P initialization failed!", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                }
                P2PUtils.stop();
            }
        } 
    }

    private static void load() throws IOException {
        String path = System.getProperty("server.conf", "server.properties");
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
        privateKey = p.getProperty("privateKey");
        if (privateKey == null || privateKey.trim().isEmpty()) {
            throw new IOException("The 'privateKey' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("port").trim();
            port = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'port' parameter is not configured.");
        }
    }
}
