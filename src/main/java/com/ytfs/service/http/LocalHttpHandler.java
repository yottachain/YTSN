package com.ytfs.service.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.TotalReq;
import com.ytfs.service.packet.bp.TotalResp;
import com.ytfs.service.packet.bp.UserListReq;
import com.ytfs.service.packet.bp.UserListResp;
import com.ytfs.service.packet.bp.UserSpace;
import com.ytfs.service.packet.bp.UserSpace.UserSpaceComparator;
import com.ytfs.service.packet.bp.UserSpaceReq;
import com.ytfs.service.packet.bp.UserSpaceResp;
import com.ytfs.service.servlet.bp.UserStatHandler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

public class LocalHttpHandler extends HttpHandler {

    private static final Logger LOG = Logger.getLogger(LocalHttpHandler.class);

    static final String REQ_TOTAL_PATH = "/total";
    static final String REQ_USER_TOTAL_PATH = "/usertotal";
    static final String REQ_USER_LIST_PATH = "/list";
    static final String REQ_ACTIVE_NODES_PATH = "/active_nodes";
    static final String REQ_STAT_PATH = "/statistics";
    static final String REQ_RELATION_SHIP_PATH = "/relationship";
    static final String REQ_NEW_NODEID = "/newnodeid";
    static final String REQ_PRE_REGNODE = "/preregnode";
    static final String REQ_CHG_MPOOL = "/changeminerpool";

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        try {
            rspns.setContentType("text/json");
            String path = rqst.getContextPath();
            if (path.equalsIgnoreCase(REQ_TOTAL_PATH)) {
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String json = gettotal();
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_RELATION_SHIP_PATH)) {
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                
                
                rspns.getWriter().write("OK");
            } else if (path.equalsIgnoreCase(REQ_USER_TOTAL_PATH)) {
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String username = rqst.getParameter("username");
                rspns.getWriter().write(getusertotal(username));
            } else if (path.equalsIgnoreCase(REQ_USER_LIST_PATH)) {
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String lastId = rqst.getParameter("lastId");
                String countstr = rqst.getParameter("count");
                int lId = -1;
                int count = 1000;
                try {
                    lId = Integer.parseInt(lastId);
                } catch (Exception r) {
                }
                try {
                    count = Integer.parseInt(countstr);
                } catch (Exception r) {
                }
                rspns.getWriter().write(listuser(lId, count));
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

    private void doRelationship(String user) {

    }

    private String gettotal() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("userTotal", UserAccessor.getUserCount());
        TotalReq req = new TotalReq();
        Object[] objs = SNSynchronizer.syncRequest(req, -1, 3);
        TotalResp resp = new TotalResp();
        for (Object obj : objs) {
            TotalResp response = (TotalResp) obj;
            resp.addTotalResp(response);
        }
        resp.putNode(node);
        return mapper.writeValueAsString(node);
    }

    private String listuser(int lId, int count) throws Exception {
        UserListReq req = new UserListReq();
        req.setLastId(lId);
        req.setCount(count);
        Object[] objs = SNSynchronizer.syncRequest(req, -1, 3);
        List<UserSpace> ls = new ArrayList();
        for (Object obj : objs) {
            UserListResp resp = (UserListResp) obj;
            List<UserSpace> list = resp.getList();
            if (list != null) {
                if (!list.isEmpty()) {
                    ls.addAll(list);
                }
            }
        }
        Collections.sort(ls, new UserSpaceComparator());
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode node = mapper.createArrayNode();
        ls.forEach((space) -> {
            ObjectNode map = node.addObject();
            map.put("userId", space.getUserId());
            map.put("userName", space.getUserName());
            map.put("spaceTotal", space.getSpaceTotal());
        });
        String json = mapper.writeValueAsString(node);
        return json;
    }

    private String getusertotal(String username) throws Exception {
        User user = UserAccessor.getUser(username);
        if (user == null) {
            throw new Exception("Invalid username:" + username);
        }
        UserSpaceReq req = new UserSpaceReq();
        req.setUserid(user.getUserID());
        SuperNode sn = SuperNodeList.getUserSuperNode(user.getUserID());
        if (sn.getId() == ServerConfig.superNodeID) {
            return UserStatHandler.query(user.getUserID());
        } else {
            UserSpaceResp resp = (UserSpaceResp) P2PUtils.requestBP(req, sn);
            return resp.getJson();
        }
    }

    private static boolean checkIp(String ip) {
        for (String mask : HttpServerBoot.ipList) {
            if (mask.trim().isEmpty()) {
                continue;
            }
            if (!ip.matches(mask)) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws IOException {
        HttpServerBoot.ipList = new String[]{"192.168.1.21"};
        System.out.println(checkIp("192.168.1.21"));
    }
}
