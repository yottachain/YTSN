package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.UserListReq;
import com.ytfs.service.packet.bp.UserListResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;

public class UserListHandler extends Handler<UserListReq> {

    private static final Logger LOG = Logger.getLogger(UserListHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        UserListResp resp = new UserListResp();
        resp.setList(UserAccessor.getUserList(request.getLastId(), request.getCount()));
        return resp;
    }

}
