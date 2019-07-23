package com.ytfs.service.dao;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Proxy {

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

    public byte[] post(byte[] data) throws IOException {
        RequestBody body = RequestBody.create(null, data);
        Request request = new Request.Builder()
                .url(uri)
                .post(body).build();
        Call call = client.newCall(request);
        Response response = call.execute();
        if (response.body() == null) {
            return null;
        } else {
            return response.body().bytes();
        }
    }

}
