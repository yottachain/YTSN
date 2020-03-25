package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.packet.user.QueryUserResp;
import com.ytfs.service.packet.v2.QueryUserReqV2;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.p2phost.utils.Base58;
import java.util.Arrays;
import org.apache.log4j.Logger;

public class QueryUserHandler extends Handler<QueryUserReqV2> {

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

    public static Object queryAndReg(QueryUserReqV2 req) throws ServiceException {
        try {
            if (req.getUserId() == -1) {
                EOSClient.getBalance(req.getUsername());
                LOG.info("[" + req.getUsername() + "] Certification passed.");
            }
        } catch (Throwable e) {
            LOG.error("", e);
            return new ServiceException(SERVER_ERROR);
        }
        byte[] KUEp = Base58.decode(req.getPubkey());
        User user = UserAccessor.getUser(req.getUsername());
        int keyNumber = 0;
        if (user != null) {
            if (req.getUserId() != -1 && req.getUserId() != user.getUserID()) {//不大可能
                LOG.error("UserID '" + user.getUserID() + "' invalid.");
                return new ServiceException(SERVER_ERROR);
            }
            byte[][] kueps = user.getKUEp();
            boolean exists = false;
            for (int ii = 0; ii < kueps.length; ii++) {
                byte[] bs = kueps[ii];
                if (Arrays.equals(bs, KUEp)) {
                    keyNumber = ii;
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                UserAccessor.updateUser(user.getUserID(), KUEp);
                byte[][] nkueps = new byte[kueps.length + 1][];
                System.arraycopy(kueps, 0, nkueps, 0, kueps.length);
                nkueps[kueps.length] = KUEp;
                user.setKUEp(nkueps);
                keyNumber = kueps.length;
            }
        } else {
            if (req.getUserId() == -1) {
                user = new User(Sequence.generateUserID());
            } else {
                user = new User(req.getUserId());
            }
            user.setKUEp(new byte[][]{KUEp});
            user.setUsername(req.getUsername());
            UserAccessor.addUser(user);
            keyNumber = 0;
        }
        QueryUserResp resp = new QueryUserResp();
        resp.setUserId(user.getUserID());
        resp.setKeyNumber(keyNumber);
        UserCache.putUser(user.getUserID(), keyNumber, user);
        return resp;
    }
}
