package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.AddDNIReq;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class AddDNIHandler extends Handler<AddDNIReq> {

    private static final Logger LOG = Logger.getLogger(AddDNIHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        if (request.isDelete()) {
            try {
                YottaNodeMgmt.deleteDNI(request.getNodeid(), request.getDni());
            } catch (NodeMgmtException ne) {
                LOG.error("DeleteDNI " + request.getNodeid() + "-[" + Hex.encodeHexString(request.getDni()) + "] ERR:" + ne.getMessage());
            }
        } else {
            try {
                YottaNodeMgmt.addDNI(request.getNodeid(), request.getDni());
            } catch (NodeMgmtException ne) {
                LOG.error("PutDNI " + request.getNodeid() + "-[" + Hex.encodeHexString(request.getDni()) + "] ERR:" + ne.getMessage());
            }
        }
        return new VoidResp();
    }

}
