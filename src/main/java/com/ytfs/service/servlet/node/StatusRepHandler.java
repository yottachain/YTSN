package com.ytfs.service.servlet.node;

import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.Node;
import org.apache.log4j.Logger;

public class StatusRepHandler extends Handler<StatusRepReq> {

    private static final Logger LOG = Logger.getLogger(StatusRepHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid = this.getNodeId();
        LOG.info("StatusRep Node:" + request.getId());
        Node node = YottaNodeMgmt.updateNodeStatus(nodeid, request.getCpu(), request.getMemory(), request.getBandwidth(),
                request.getMaxDataSpace(), request.getAddrs());
        StatusRepResp resp = new StatusRepResp();
        resp.setProductiveSpace(node.getProductiveSpace());
        return resp;
    }

}
