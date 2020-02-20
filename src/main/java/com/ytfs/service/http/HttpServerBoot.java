package com.ytfs.service.http;

import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.httpServlet;
import static com.ytfs.service.http.LocalHttpHandler.REQ_ACTIVE_NODES_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_1;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_2;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_3;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_4;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_5;
import static com.ytfs.service.http.LocalHttpHandler.REQ_API_6;
import static com.ytfs.service.http.LocalHttpHandler.REQ_CHG_MPOOL;
import static com.ytfs.service.http.LocalHttpHandler.REQ_NEW_NODEID;
import static com.ytfs.service.http.LocalHttpHandler.REQ_PRE_REGNODE;
import static com.ytfs.service.http.LocalHttpHandler.REQ_QUERY_VHF;
import static com.ytfs.service.http.LocalHttpHandler.REQ_RELATION_SHIP_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_STAT_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_TOTAL_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_USER_LIST_PATH;
import static com.ytfs.service.http.LocalHttpHandler.REQ_USER_TOTAL_PATH;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
            List<String> servlets = new ArrayList();
            servlets.add("/");
            servlets.add(REQ_TOTAL_PATH);
            servlets.add(REQ_USER_TOTAL_PATH);
            servlets.add(REQ_ACTIVE_NODES_PATH);
            servlets.add(REQ_RELATION_SHIP_PATH);
            servlets.add(REQ_USER_LIST_PATH);
            servlets.add(REQ_STAT_PATH);
            servlets.add(REQ_NEW_NODEID);
            servlets.add(REQ_PRE_REGNODE);
            servlets.add(REQ_CHG_MPOOL);
            servlets.add(REQ_API_1);
            servlets.add(REQ_API_2);
            servlets.add(REQ_API_3);
            servlets.add(REQ_API_4);
            servlets.add(REQ_API_5);
            servlets.add(REQ_API_6);
            if (httpServlet != null && !httpServlet.isEmpty()) {
                REQ_QUERY_VHF = httpServlet.startsWith("/") ? httpServlet : ("/" + httpServlet);
                servlets.add(REQ_QUERY_VHF);
            }
            httpServer.getServerConfiguration().addHttpHandler(httpHandler, servlets.toArray(new String[servlets.size()]));
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
