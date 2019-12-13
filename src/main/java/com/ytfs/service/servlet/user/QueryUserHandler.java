package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.INVALID_USER_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.packet.user.QueryUserReq;
import com.ytfs.service.packet.user.QueryUserResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.utils.Base58;
import java.util.Arrays;
import org.apache.log4j.Logger;

public class QueryUserHandler extends Handler<QueryUserReq> {

    private static final Logger LOG = Logger.getLogger(QueryUserHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        LOG.debug("User '" + request.getUsername() + "' sync request.");
        return queryAndReg(request);
    }

    public static QueryUserResp queryAndReg(QueryUserReq req) throws ServiceException {
        byte[] KUEp = Base58.decode(req.getPubkey());
        User user = UserAccessor.getUser(req.getUsername());
        if (user != null) {
            if (!Arrays.equals(KUEp, user.getKUEp())) {
                LOG.error("User pubkey '" + req.getPubkey() + "' invalid.");
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
        UserCache.putUser(req.getCacheKey(), user);
        return resp;
    }
}
