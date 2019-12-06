package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.DNIAccessor;
import com.ytfs.service.packet.node.ListDNIReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class DNICheckHandler extends Handler<ListDNIReq> {

    private static final Logger LOG = Logger.getLogger(DNICheckHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (Throwable e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        return DNIAccessor.listDNI(nodeid, request.getNextId(), request.getCount());
    }

}
