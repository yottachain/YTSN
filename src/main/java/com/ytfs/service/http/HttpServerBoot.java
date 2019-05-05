package com.ytfs.service.http;

import com.ytfs.service.ServerConfig;
import java.io.IOException;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;

public class HttpServerBoot {

    static HttpServer httpServer;

    public static void startHttpServer() throws IOException {
        if (httpServer == null) {
            httpServer = new HttpServer();
            NetworkListener networkListener = new NetworkListener("ytsn", "127.0.0.1", ServerConfig.httpPort);
            ThreadPoolConfig threadPoolConfig = ThreadPoolConfig.defaultConfig().setCorePoolSize(2).setMaxPoolSize(20);
            networkListener.getTransport().setWorkerThreadPoolConfig(threadPoolConfig);
            httpServer.addListener(networkListener);
            HttpHandler httpHandler = new UseSpaceHandler();
            httpServer.getServerConfiguration().addHttpHandler(httpHandler, new String[]{"/usespace"});
            httpServer.start();
        }
    }

    public static void stopHttpServer()  {
        if (httpServer != null) {
            httpServer.shutdown();
        }
    }
}
