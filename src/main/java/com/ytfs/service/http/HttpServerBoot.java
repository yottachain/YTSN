package com.ytfs.service.http;

import com.ytfs.service.ServerConfig;
import static com.ytfs.service.http.UseSpaceHandler.REQ_TOTAL_PATH;
import static com.ytfs.service.http.UseSpaceHandler.REQ_USER_PATH;
import java.io.IOException;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;

public class HttpServerBoot {

    static HttpServer httpServer = null;

    public static void startHttpServer() throws IOException {
        if (httpServer == null) {
            httpServer = new HttpServer();
            String ip = ServerConfig.httpBindip;
            if (ip == null || ip.isEmpty()) {
                ip = "0.0.0.0";
            }
            NetworkListener networkListener = new NetworkListener("ytsn", ip, ServerConfig.httpPort);
            ThreadPoolConfig threadPoolConfig = ThreadPoolConfig.defaultConfig().setCorePoolSize(2).setMaxPoolSize(20);
            networkListener.getTransport().setWorkerThreadPoolConfig(threadPoolConfig);
            httpServer.addListener(networkListener);
            HttpHandler httpHandler = new UseSpaceHandler();
            httpServer.getServerConfiguration().addHttpHandler(httpHandler, new String[]{"/", REQ_USER_PATH, REQ_TOTAL_PATH});
            httpServer.start();
        }
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
