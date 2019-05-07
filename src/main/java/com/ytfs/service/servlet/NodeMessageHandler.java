package com.ytfs.service.servlet;

import com.ytfs.service.packet.NodeRegReq;
import com.ytfs.service.packet.NodeRegResp;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.Node;
import org.apache.log4j.Logger;

public class NodeMessageHandler {

    private static final Logger LOG = Logger.getLogger(NodeMessageHandler.class);

    static NodeRegResp reg(NodeRegReq req) throws ServiceException, Throwable {
        LOG.info("Reg Node:" + req.getNodeid());
        LOG.info("Reg getOwner:" + req.getOwner());
        LOG.info("Reg getAddrs:" + req.getAddrs().size());
        LOG.info("Reg getMaxDataSpace:" + req.getMaxDataSpace());
        Node node = YottaNodeMgmt.registerNode(req.getNodeid(), req.getOwner(), req.getMaxDataSpace(), req.getAddrs());
        NodeRegResp resp = new NodeRegResp();
        resp.setId(node.getId());
        resp.setAssignedSpace(node.getAssignedSpace());
        LOG.info("Reg getId:" + node.getId());
        LOG.info("Reg getAssignedSpace:" + node.getAssignedSpace());
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
