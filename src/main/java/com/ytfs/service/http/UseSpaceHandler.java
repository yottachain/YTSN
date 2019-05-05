package com.ytfs.service.http;

import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class UseSpaceHandler extends HttpHandler {

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        rspns.setContentType("text/plain");
        rspns.getWriter().write("Complex task is done!");
    }

}
