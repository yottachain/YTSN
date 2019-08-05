package com.ytfs.service.servlet.user;

import static com.ytfs.common.ServiceErrorCode.INVALID_USER_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.user.QueryUserReq;
import com.ytfs.service.packet.user.QueryUserResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.p2phost.utils.Base58;
import org.apache.log4j.Logger;

public class QueryUserHandler extends Handler<QueryUserReq> {

    private static final Logger LOG = Logger.getLogger(QueryUserHandler.class);

    @Override
    public Object handle() throws Throwable {
        return queryAndReg(request);
    }

    public static QueryUserResp queryAndReg(QueryUserReq req) throws ServiceException {
        byte[] KUEp = Base58.decode(req.getPubkey());
        User user = UserAccessor.getUser(KUEp);
        if (user != null) {
            if (!user.getUsername().equals(req.getUsername())) {//不大可能
                LOG.error("Username '" + user.getUsername() + "' invalid.");
                throw new ServiceException(INVALID_USER_ID);
            }
            if (req.getUserId() != -1 && req.getUserId() != user.getUserID()) {//不大可能
                LOG.error("UserID '" + user.getUserID() + "' invalid.");
                throw new ServiceException(INVALID_USER_ID);
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
