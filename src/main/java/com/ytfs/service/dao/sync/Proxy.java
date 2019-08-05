package com.ytfs.service.dao.sync;

import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.log4j.Logger;

public class Proxy {

    private static final Logger LOG = Logger.getLogger(Proxy.class);
    private final String uri;
    private final OkHttpClient client;

    public Proxy(String uri) {
        this.uri = uri;
        this.client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(15, TimeUnit.SECONDS)
                .writeTimeout(15, TimeUnit.SECONDS)
                .build();
    }

    public void post(LogMessage obj) {
        for (int ii = 0; ii < 2; ii++) {
            try {
                byte[] data = obj.toByte();
                RequestBody body = RequestBody.create(null, data);
                Request request = new Request.Builder().url(uri).post(body).build();
                Call call = client.newCall(request);
                call.execute();
                return;
            } catch (Throwable r) {
                LOG.error("Post ERR:" + r.getMessage());
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

}
