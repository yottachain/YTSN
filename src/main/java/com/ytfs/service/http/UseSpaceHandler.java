package com.ytfs.service.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.service.dao.UserAccessor;
import java.io.InputStream;
import org.bson.Document;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

public class UseSpaceHandler extends HttpHandler {

    static final String REQ_USER_PATH = "/user";
    static final String REQ_TOTAL_PATH = "/total";

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        try {
            rspns.setContentType("text/json");
            String path = rqst.getContextPath();
            if (path.equalsIgnoreCase(REQ_USER_PATH)) {
                String uri = rqst.getRequestURI();
                String userid = uri.replaceFirst(REQ_USER_PATH, "");
                String json = getusertotal(userid);
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_TOTAL_PATH)) {
                String json = gettotal();
                rspns.getWriter().write(json);
            } else {
                rspns.setContentType("text/html");
                InputStream is = this.getClass().getResourceAsStream("/statapi.html");
                byte[] bs = new byte[1024];
                int len = 0;
                while ((len = is.read(bs)) != -1) {
                    rspns.getOutputStream().write(bs, 0, len);
                }
            }
            rspns.flush();
        } catch (Exception e) {
            String message = e.getMessage();
            rspns.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500.getStatusCode(), message);
        }
    }

    
    private String gettotal() throws Exception {
        Document doc = UserAccessor.total();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(doc);
    }

    public static String getusertotal(String id) throws Exception {
        int userid = 0;
        try {
            userid = Integer.parseInt(id.substring(1));
        } catch (Exception r) {
            throw new Exception("Invalid userid");
        }
        Document doc = UserAccessor.userTotal(userid);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(doc);
    }

}
