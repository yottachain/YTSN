package com.ytfs.service.servlet;

import com.ytfs.service.packet.NodeRegReq;
import com.ytfs.service.packet.NodeRegResp;
import com.ytfs.service.utils.ServiceException;
import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.Node;
import org.apache.log4j.Logger;

public class NodeMessageHandler {

    private static final Logger LOG = Logger.getLogger(NodeMessageHandler.class);

    static NodeRegResp reg(NodeRegReq req, String pubkey) throws ServiceException, Throwable {
        LOG.info("Reg Node:" + req.getNodeid());
        Node node = YottaNodeMgmt.registerNode(req.getNodeid(), pubkey, req.getOwner(), req.getMaxDataSpace(), req.getAddrs());
        NodeRegResp resp = new NodeRegResp();
        resp.setId(node.getId());
        resp.setAssignedSpace(node.getAssignedSpace());
        LOG.info("Node Registered,Id:" + node.getId());
        return resp;
    }

    static StatusRepResp statusRep(StatusRepReq req, int nodeid) throws ServiceException, Throwable {
        LOG.info("StatusRep Node:" + req.getId());
        Node node = YottaNodeMgmt.updateNodeStatus(nodeid, req.getCpu(), req.getMemory(), req.getBandwidth(),
                req.getMaxDataSpace(), req.getAssignedSpace(), req.getUsedSpace(), req.getAddrs());
        StatusRepResp resp = new StatusRepResp();
        resp.setProductiveSpace(node.getProductiveSpace());
        return resp;
    }
}
