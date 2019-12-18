package com.ytfs.service.http;

import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.service.http.LocalHttpHandler.REQ_ACTIVE_NODES_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_CHG_MPOOL;
import static com.ytfs.service.http.LocalHttpHandler.REQ_NEW_NODEID;
import static com.ytfs.service.http.LocalHttpHandler.REQ_PRE_REGNODE;
import static com.ytfs.service.http.LocalHttpHandler.REQ_STAT_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_TOTAL_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_USER_LIST_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_USER_TOTAL_PATH;
import java.io.IOException;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;

public class HttpServerBoot {

    static HttpServer httpServer = null;
    static String[] ipList;

    public static void startHttpServer() throws IOException {
        if (httpServer == null) {
            httpServer = new HttpServer();
            NetworkListener networkListener = new NetworkListener("ytsn", "0.0.0.0", ServerConfig.httpPort);
            ThreadPoolConfig threadPoolConfig = ThreadPoolConfig.defaultConfig().setCorePoolSize(2).setMaxPoolSize(20);
            networkListener.getTransport().setWorkerThreadPoolConfig(threadPoolConfig);
            httpServer.addListener(networkListener);
            HttpHandler httpHandler = new LocalHttpHandler();
            httpServer.getServerConfiguration().addHttpHandler(httpHandler,
                    new String[]{"/",
                        REQ_TOTAL_PATH,
                        REQ_USER_TOTAL_PATH,
                        REQ_ACTIVE_NODES_PATH,
                        REQ_USER_LIST_PATH,
                        REQ_STAT_PATH,
                        REQ_NEW_NODEID,
                        REQ_PRE_REGNODE,
                        REQ_CHG_MPOOL});
            httpServer.start();
        }
        ipList = ServerConfig.httpRemoteIp.split(";");
    }

    public static void stopHttpServer() {
        if (httpServer != null) {
            httpServer.shutdown();
        }
    }

    public static void main(String[] args) throws IOException {
        startHttpServer();
        System.in.read();
        stopHttpServer();
    }
}
