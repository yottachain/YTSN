package com.ytfs.service.servlet.bp;

import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.NodeSyncReq;

import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import org.apache.log4j.Logger;

public class NodeSyncHandler extends Handler<NodeSyncReq> {

    private static final Logger LOG = Logger.getLogger(NodeSyncHandler.class);

    @Override
    public VoidResp handle() throws Throwable {
        LOG.debug("Sync Node:" + request.getNode().getId());
        YottaNodeMgmt.syncNode(request.getNode());
        return new VoidResp();
    }

}
