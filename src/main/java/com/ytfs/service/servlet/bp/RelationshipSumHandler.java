package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.RelationShipSum;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;

public class RelationshipSumHandler extends Handler<RelationShipSum> {
    
    private static final Logger LOG = Logger.getLogger(RelationshipSumHandler.class);
    
    @Override
    public Object handle() throws Throwable {
        int snid;
        try {
            snid = getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        if (request.getMowner() != null) {
            for (int ii = 0, len = request.getMowner().size(); ii < len; ii++) {
                try {
                    UserAccessor.updateRelationshipCache(snid, request.getMowner().get(ii), request.getUsedspace().get(ii));
                } catch (Throwable r) {
                    LOG.error("ERR:" + r.getMessage());
                }
            }
        }
        return new VoidResp();
    }
}
