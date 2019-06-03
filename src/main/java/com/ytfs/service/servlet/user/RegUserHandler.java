package com.ytfs.service.servlet.user;

import static com.ytfs.common.ServiceErrorCode.INVALID_USER_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.QueryUserReq;
import com.ytfs.service.packet.QueryUserResp;
import com.ytfs.service.packet.RegUserReq;
import com.ytfs.service.packet.RegUserResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.utils.Base58;

public class RegUserHandler extends Handler<RegUserReq> {

    public static class QueryUserHandler extends Handler<QueryUserReq> {

        @Override
        public Object handle() throws Throwable {
            return queryAndReg(request);
        }
    }

    private static QueryUserResp queryAndReg(QueryUserReq req) throws ServiceException {
        byte[] KUEp = Base58.decode(req.getPubkey());
        User user = UserAccessor.getUser(KUEp);
        if (user != null) {
            if (!user.getUsername().equals(req.getUsername())) {
                UserAccessor.updateUserName(user.getUserID(), req.getUsername());
                user.setUsername(req.getUsername());
            }
            if (req.getUserId() != -1 && req.getUserId() != user.getUserID()) {//不大可能
                int oldid = user.getUserID();
                user.setUserID(req.getUserId());
                UserAccessor.deleteAndAddUser(oldid, user);
            }
        } else {
            if (req.getUserId() == -1) {
                try {//需要验证用户
                    EOSClient.getBalance(req.getUsername());
                } catch (Throwable e) {
                    throw new ServiceException(INVALID_USER_ID);
                }
                user = new User(Sequence.generateUserID());
            } else {
                user = new User(req.getUserId());
            }
            user.setKUEp(KUEp);
            user.setUsername(req.getUsername());
            UserAccessor.addUser(user);
        }
        QueryUserResp resp = new QueryUserResp();
        resp.setUserId(user.getUserID());
        return resp;
    }

    @Override
    public Object handle() throws Throwable {
        byte[] KUEp = Base58.decode(request.getPubkey());
        SuperNode sn = SuperNodeList.getUserRegSuperNode(KUEp);
        QueryUserReq req = new QueryUserReq();
        req.setPubkey(request.getPubkey());
        req.setUsername(request.getUsername());
        QueryUserResp resp;
        if (sn.getId() == ServerConfig.superNodeID) {
            resp = queryAndReg(req);
        } else {
            resp = (QueryUserResp) P2PUtils.requestBP(req, sn);
        }
        req.setUserId(resp.getUserId());
        SuperNode[] snlist = SuperNodeList.getSuperNodeList();
        for (SuperNode node : snlist) {
            if (node.getId() == sn.getId()) {
                continue;
            }
            if (node.getId() == ServerConfig.superNodeID) {
                queryAndReg(req);
            } else {
                P2PUtils.requestBP(req, node);
            }
        }
        RegUserResp regUserResp = new RegUserResp();
        SuperNode ressn = SuperNodeList.getUserSuperNode(resp.getUserId());
        regUserResp.setSuperNodeAddrs(ressn.getAddrs());
        regUserResp.setSuperNodeID(ressn.getNodeid());
        regUserResp.setSuperNodeNum(ressn.getId());
        return regUserResp;
    }

}
