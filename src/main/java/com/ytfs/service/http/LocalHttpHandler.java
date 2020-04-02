package com.ytfs.service.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.dao.DNIAccessor;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.Relationship;
import com.ytfs.service.packet.bp.TotalReq;
import com.ytfs.service.packet.bp.TotalResp;
import com.ytfs.service.packet.bp.UserListReq;
import com.ytfs.service.packet.bp.UserListResp;
import com.ytfs.service.packet.bp.UserSpace;
import com.ytfs.service.packet.bp.UserSpace.UserSpaceComparator;
import com.ytfs.service.packet.bp.UserSpaceReq;
import com.ytfs.service.packet.bp.UserSpaceResp;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.HandlerFactory;
import com.ytfs.service.servlet.bp.UserStatHandler;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.ApiName;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.ErrorPageGenerator;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

public class LocalHttpHandler extends HttpHandler {

    private static final Logger LOG = Logger.getLogger(LocalHttpHandler.class);

    static final String REQ_TOTAL_PATH = "/total";//可验证
    static final String REQ_USER_TOTAL_PATH = "/usertotal";//可验证
    static final String REQ_USER_LIST_PATH = "/list";//可验证
    static final String REQ_ACTIVE_NODES_PATH = "/active_nodes"; 
    static final String REQ_STAT_PATH = "/statistics";
    static final String REQ_RELATION_SHIP_PATH = "/relationship";
    static final String REQ_NEW_NODEID = "/newnodeid";
    static final String REQ_PRE_REGNODE = "/preregnode";
    static final String REQ_CHG_MPOOL = "/changeminerpool";
    static final String REQ_API_1 = "/ChangeAdminAcc";
    static final String REQ_API_2 = "/ChangeProfitAcc";
    static final String REQ_API_3 = "/ChangePoolID";
    static final String REQ_API_4 = "/ChangeAssignedSpace";
    static final String REQ_API_5 = "/ChangeDepAcc";
    static final String REQ_API_6 = "/ChangeDeposit";
    static String REQ_QUERY_VHF = "/findvhf";

    @Override
    public void service(Request rqst, Response rspns) throws Exception {
        try {
            rspns.setCharacterEncoding("UTF-8");
            String path = rqst.getContextPath();
            if (path.equalsIgnoreCase(REQ_TOTAL_PATH)) {
                rspns.setContentType("text/json");
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String json = gettotal();
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_RELATION_SHIP_PATH)) {
                rspns.setContentType("text/plain");
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String username = rqst.getParameter("username");
                String mPoolOwner = rqst.getParameter("mPoolOwner");
                doRelationship(username, mPoolOwner);
                rspns.getWriter().write("OK");
            } else if (path.equalsIgnoreCase(REQ_USER_TOTAL_PATH)) {
                rspns.setContentType("text/json");
                if (!checkIp(rqst.getRemoteAddr())) {
                    throw new Exception("Invalid IP:" + rqst.getRemoteAddr());
                }
                String username = rqst.getParameter("username");
                rspns.getWriter().write(getusertotal(username));
            } else if (path.equalsIgnoreCase(REQ_USER_LIST_PATH)) {
                rspns.setContentType("text/json");
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
                rspns.setContentType("text/json");
                List<Map<String, String>> ls = YottaNodeMgmt.activeNodesList();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(ls);
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_STAT_PATH)) {
                rspns.setContentType("text/json");
                Map<String, Long> map = YottaNodeMgmt.statistics();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(map);
                rspns.getWriter().write(json);
            } else if (path.equalsIgnoreCase(REQ_NEW_NODEID)) {
                rspns.setContentType("text/json");
                int id = YottaNodeMgmt.newNodeID();
                String res = "{\"nodeid\": " + id + "}";
                rspns.getWriter().write(res);
            } else if (path.equalsIgnoreCase(REQ_PRE_REGNODE)) {
                callApi(rqst, rspns, ApiName.PreRegisterNode);
            } else if (path.equalsIgnoreCase(REQ_CHG_MPOOL)) {
                callApi(rqst, rspns, ApiName.ChangeMinerPool);
            } else if (path.equalsIgnoreCase(REQ_API_1)) {
                callApi(rqst, rspns, ApiName.ChangeAdminAcc);
            } else if (path.equalsIgnoreCase(REQ_API_2)) {
                callApi(rqst, rspns, ApiName.ChangeProfitAcc);
            } else if (path.equalsIgnoreCase(REQ_API_3)) {
                callApi(rqst, rspns, ApiName.ChangePoolID);
            } else if (path.equalsIgnoreCase(REQ_API_4)) {
                callApi(rqst, rspns, ApiName.ChangeAssignedSpace);
            } else if (path.equalsIgnoreCase(REQ_API_5)) {
                callApi(rqst, rspns, ApiName.ChangeDepAcc);
            } else if (path.equalsIgnoreCase(REQ_API_6)) {
                callApi(rqst, rspns, ApiName.ChangeDeposit);
            } else if (path.equalsIgnoreCase(REQ_QUERY_VHF)) {
                rspns.setContentType("text/plain");
                String res = lookup(rqst);
                rspns.getWriter().write(res);
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
            rspns.setContentType("text/plain");
            rspns.setErrorPageGenerator(new ErrorPageGenerator() {
                @Override
                public String generate(final Request request,
                        final int status, final String reasonPhrase,
                        final String description, final Throwable exception) {
                    return description;
                }
            });
            rspns.sendError(HttpStatus.INTERNAL_SERVER_ERROR_500.getStatusCode(), message);
        }
    }

    private void callApi(Request rqst, Response rspns, ApiName apiname) throws Exception {
        rspns.setContentType("text/plain");
        if (rqst.getMethod() == Method.POST) {
            InputStream is = new BufferedInputStream(rqst.getInputStream());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            int b = 0;
            while ((b = is.read()) != -1) {
                os.write(b);
            }
            String trx = new String(os.toByteArray(), "utf-8");
            LOG.info("Call API:" + apiname + ",trx:" + trx);
            YottaNodeMgmt.callAPI(trx, apiname);
            rspns.getWriter().write("OK");
        } else {
            rspns.sendError(500);
        }
    }

    private String lookup(Request rqst) throws Exception {
        String vhf = rqst.getParameter("vhf");
        String nodeid = rqst.getParameter("nodeid");
        int id = 0;
        try {
            id = Integer.parseInt(nodeid);
        } catch (Throwable r) {
            throw new Exception("Invalid nodeid:" + nodeid);
        }
        if (SuperNodeList.getNodeSuperNode(id).getId() != ServerConfig.superNodeID) {
            throw new Exception("ID '" + nodeid + "' does not belong to current SN management.");
        }
        byte[] VHF;
        try {
            VHF = Base58.decode(vhf);
        } catch (Throwable r) {
            throw new Exception("Invalid VHF:" + vhf);
        }
        if (VHF.length != 16) {
            throw new Exception("Invalid VHF:" + vhf + ",len:" + VHF.length);
        }
        boolean b = DNIAccessor.findDNI(id, VHF);
        return b ? "True" : "False";
    }

    private void doRelationship(String user, String mPoolOwner) {
        if (user == null || mPoolOwner == null || user.trim().isEmpty() || mPoolOwner.trim().isEmpty()) {
            return;
        }
        SuperNode node = SuperNodeList.getUserRegSuperNode(user);
        Relationship relationship = new Relationship();
        relationship.setUsername(user);
        relationship.setMpoolOwner(mPoolOwner);
        try {
            if (node.getId() == ServerConfig.superNodeID) {
                Handler handler = HandlerFactory.getHandler(relationship);
                String pubkey = node.getPubkey();
                if (pubkey.startsWith("EOS")) {
                    pubkey = pubkey.substring(3);
                }
                handler.setPubkey(pubkey);
                handler.handle();
            } else {
                P2PUtils.requestBP(relationship, node);
            }
        } catch (Throwable r) {
            LOG.error("DoRelationship err:" + r.getMessage());
        }
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
        if(HttpServerBoot.ipList==null||HttpServerBoot.ipList.length==0){
            return true;
        }
        for (String mask : HttpServerBoot.ipList) {
            if (mask.trim().isEmpty()) {
                continue;
            }
            if (ip.matches(mask)) {
                return true;
            }
        }
        return false;
    }
}
