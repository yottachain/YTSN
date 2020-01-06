package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.Relationship;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;

public class RelationshipHandler extends Handler<Relationship> {

    private static final Logger LOG = Logger.getLogger(RelationshipHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        UserAccessor.updateRelationship(request.getMpoolOwner(), request.getUsername());
        return new VoidResp();
    }
}
