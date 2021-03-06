package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.DNIAccessor;
import com.ytfs.service.packet.node.ListDNIReq;
import com.ytfs.service.packet.node.ListDNIResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class DNICheckHandler extends Handler<ListDNIReq> {

    private static final Logger LOG = Logger.getLogger(DNICheckHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        LOG.info("Request:" + (request.getNextId() == null ? "" : request.getNextId()) + ",count " + request.getCount());
        try {
            nodeid = this.getNodeId();
        } catch (Throwable e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        LOG.info("Form " + nodeid + " DNI ls req:" + (request.getNextId() == null ? "" : request.getNextId()));
        ListDNIResp res = DNIAccessor.listDNI(nodeid, request.getNextId(), request.getCount());
        LOG.info("To " + nodeid + " DNI ls resp:" + (res.getNextId() == null ? "" : res.getNextId()));
        return res;
    }

}
