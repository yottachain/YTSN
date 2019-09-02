package com.ytfs.service.http;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class HttpClient {

    public static final MediaType mediaType = MediaType.parse("application/octet-stream");
    private static final String toSnUrl = "https://localhost:8080/tosn";
    private static final String toNodeUrl = "https://localhost:8080/tonode";

    private static OkHttpClient client;

    public static void initClient() throws Exception {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        client = builder.build();
    }

    public byte[] sendMsgToSN(byte[] bs) throws Exception {
        final MediaType mediaType = MediaType.parse("application/octet-stream");
        final String toSnUrl = "https://localhost:8080/tosn?snid=0";
        RequestBody body = RequestBody.create(mediaType, bs);
        Request request = new Request.Builder().url(toSnUrl).post(body).build();
        Response response = client.newCall(request).execute();
        byte[] res = response.body().bytes();
        return res;
    }
}
