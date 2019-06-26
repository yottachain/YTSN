package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.user.QueryUserReq;
import com.ytfs.service.packet.user.QueryUserResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.p2phost.utils.Base58;

public class QueryUserHandler extends Handler<QueryUserReq> {

    @Override
    public Object handle() throws Throwable {
        return queryAndReg(request);
    }

    public static QueryUserResp queryAndReg(QueryUserReq req) throws ServiceException {
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
}
