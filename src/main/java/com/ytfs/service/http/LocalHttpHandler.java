package com.ytfs.service.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.service.dao.UserAccessor;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

public class LocalHttpHandler extends HttpHandler {

    private static final Logger LOG = Logger.getLogger(LocalHttpHandler.class);

    static final String REQ_TOTAL_PATH = "/total";
    static final String REQ_ACTIVE_NODES_PATH = "/active_nodes";
    static final String REQ_STAT_PATH = "/statistics";
    static final String REQ_NEW_NODEID = "/newnodeid";
    static final String REQ_PRE_REGNODE = "/preregnode";
    static final String REQ_CHG_MPOOL = "/changeminerpool";

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        try {
            rspns.setContentType("text/json");
            String path = rqst.getContextPath();
            if (path.equalsIgnoreCase(REQ_TOTAL_PATH)) {
                String json = gettotal();
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_ACTIVE_NODES_PATH)) {
                List<Map<String, String>> ls = YottaNodeMgmt.activeNodesList();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(ls);
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_STAT_PATH)) {
                Map<String, Long> map = YottaNodeMgmt.statistics();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(map);
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_NEW_NODEID)) {
                int id = YottaNodeMgmt.newNodeID();
                String res = "{\"nodeid\": " + id + "}";
                rspns.getWriter().write(res);
            } else if (path.equalsIgnoreCase(REQ_PRE_REGNODE)) {
                if (rqst.getMethod() == Method.POST) {
                    InputStream is = new BufferedInputStream(rqst.getInputStream());
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    int b = 0;
                    while ((b = is.read()) != -1) {
                        os.write(b);
                    }
                    String trx = new String(os.toByteArray(), "utf-8");
                    LOG.info("PreRegisterNode:" + trx);
                    YottaNodeMgmt.preRegisterNode(trx);
                    rspns.setContentType("text/plain");
                    rspns.getWriter().write("OK");
                }
            } else if (path.equalsIgnoreCase(REQ_CHG_MPOOL)) {
                if (rqst.getMethod() == Method.POST) {
                    InputStream is = new BufferedInputStream(rqst.getInputStream());
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    int b = 0;
                    while ((b = is.read()) != -1) {
                        os.write(b);
                    }
                    String trx = new String(os.toByteArray(), "utf-8");
                    LOG.info("ChangeMinerPool:" + trx);
                    YottaNodeMgmt.changeMinerPool(trx);
                    rspns.setContentType("text/plain");
                    rspns.getWriter().write("OK");
                } else {
                    rspns.sendError(500);
                }
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
        } catch (Throwable e) {
            LOG.error("", e);
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
            userid = Integer.parseInt(id);
        } catch (Exception r) {
            throw new Exception("Invalid userid");
        }
        Document doc = UserAccessor.userTotal(userid);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(doc);
    }

}
