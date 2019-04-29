package com.ytfs.service.http;

import java.io.IOException;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;

public class HttpServerBoot {

    public static void startHttpServer() throws IOException {
        HttpServer httpServer = new HttpServer();
        NetworkListener networkListener = new NetworkListener("sample-listener", "127.0.0.1", 18888);
        ThreadPoolConfig threadPoolConfig = ThreadPoolConfig
                .defaultConfig()
                .setCorePoolSize(1)
                .setMaxPoolSize(1);
        networkListener.getTransport().setWorkerThreadPoolConfig(threadPoolConfig);
        httpServer.addListener(networkListener);
        HttpHandler httpHandler = new UseSpaceHandler();
        httpServer.getServerConfiguration().addHttpHandler(httpHandler, new String[]{"/sample"});
        httpServer.start();
    }

}
